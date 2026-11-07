"""Real CLI workflows across independent relational and vector stores."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time
from dataclasses import asdict
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from typer.testing import CliRunner
from psycopg import sql as postgres_sql

from chronos_core import _native_interval
from chronos_core.cli._config import DatabaseConfig, QdrantConfig, load_workspace_config, open_workspace


def _run_cli(arguments: list[str], *, environment: dict[str, str],
             ok: bool = True, input_text: str | None = None) -> Any:
    result = subprocess.run(
        [sys.executable, "-X", "faulthandler", "-m", "chronos_core.cli", *arguments],
        env=environment, input=input_text, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == (0 if ok else 1), result.stderr
    assert "Traceback" not in result.stderr
    if not ok:
        assert result.stdout == ""
    return json.loads(result.stdout) if result.stdout.strip() else None


@pytest.fixture(scope="session")
def postgres_cli_database_url() -> Iterator[str]:
    """Create one owned database for the run, without resetting the admin database."""
    admin_url = os.environ.get("CHRONOS_CLI_TEST_POSTGRES_ADMIN_URL")
    if not admin_url:
        pytest.skip("Set CHRONOS_CLI_TEST_POSTGRES_ADMIN_URL to test PostgreSQL")
    parsed = urlsplit(admin_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        pytest.fail("CHRONOS_CLI_TEST_POSTGRES_ADMIN_URL must be a PostgreSQL URL")
    name = f"chronos_cli_test_{uuid4().hex}"
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=10) as admin:
        admin.execute(postgres_sql.SQL("CREATE DATABASE {}").format(postgres_sql.Identifier(name)))
        try:
            yield urlunsplit(parsed._replace(path=f"/{name}"))
        finally:
            admin.execute(
                postgres_sql.SQL("DROP DATABASE {} WITH (FORCE)").format(postgres_sql.Identifier(name))
            )


@pytest.fixture(params=["sqlite", "postgres", "duckdb-sqlite", "duckdb-postgres"])
def cli_database(request: pytest.FixtureRequest, tmp_path: Path) -> DatabaseConfig:
    kind = request.param
    if kind.startswith("duckdb") and not _native_interval.has_duckdb:
        if os.environ.get("CHRONOS_CLI_REQUIRE_DUCKDB") == "ON":
            pytest.fail("The CLI test matrix requires a DuckDB-enabled native build")
        pytest.skip("DuckDB disabled in this native build")
    if kind == "sqlite":
        return DatabaseConfig(f"sqlite:///{tmp_path / 'workflow.sqlite'}")
    if kind == "duckdb-sqlite":
        return DatabaseConfig(
            f"duckdb:///{tmp_path / 'workflow.duckdb'}",
            f"sqlite:///{tmp_path / 'metadata.sqlite'}",
        )
    url = request.getfixturevalue("postgres_cli_database_url")
    # This URL belongs only to the database created by our session fixture.
    with psycopg.connect(url, autocommit=True, connect_timeout=10) as database:
        database.execute("DROP SCHEMA public CASCADE")
        database.execute("CREATE SCHEMA public")
    return DatabaseConfig(
        f"duckdb:///{tmp_path / 'workflow.duckdb'}", url,
    ) if kind == "duckdb-postgres" else DatabaseConfig(url)


@pytest.fixture(params=["local-sqlite", "remote-sqlite", "local-postgres", "remote-postgres"])
def qdrant_target(request: pytest.FixtureRequest):
    client_type = pytest.importorskip("qdrant_client").QdrantClient
    from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

    kind = request.param
    remote_url = os.environ.get("CHRONOS_CLI_TEST_QDRANT_URL")
    api_key = os.environ.get("CHRONOS_CLI_TEST_QDRANT_API_KEY")
    if kind.startswith("remote") and not remote_url:
        pytest.skip("Set CHRONOS_CLI_TEST_QDRANT_URL to test the Qdrant server")
    client = None
    prefix = f"cli_test_{uuid4().hex}_"
    if kind.startswith("remote"):
        client = client_type(url=remote_url, api_key=api_key, timeout=2)
        deadline = time.monotonic() + 30
        while True:
            try:
                client.get_collections()
                break
            except (ResponseHandlingException, UnexpectedResponse):
                if time.monotonic() >= deadline:
                    client.close()
                    pytest.fail("Configured Qdrant test server is not ready after 30 seconds")
                time.sleep(0.5)
    try:
        with TemporaryDirectory(prefix="chronos-cli-qdrant-") as directory:
            root = Path(directory)
            metadata = f"sqlite:///{root / 'metadata.sqlite'}"
            if kind.endswith("postgres"):
                metadata = request.getfixturevalue("postgres_cli_database_url")
                # Only reset the database that our session fixture owns.
                with psycopg.connect(metadata, autocommit=True) as admin:
                    admin.execute("DROP SCHEMA public CASCADE")
                    admin.execute("CREATE SCHEMA public")
            yield QdrantConfig(
                metadata_url=metadata,
                path=None if kind.startswith("remote") else root / "vectors",
                url=remote_url if kind.startswith("remote") else None,
                api_key=api_key if kind.startswith("remote") else None,
                collection_prefix=prefix,
            )
    finally:
        if client is not None:
            try:
                for collection in client.get_collections().collections:
                    if collection.name.startswith(prefix):
                        client.delete_collection(collection.name)
            finally:
                client.close()


def _open_qdrant_api(target: QdrantConfig):
    from chronos_core.workspace.qdrant import ChronosQdrantStore

    if target.path is not None:
        return ChronosQdrantStore.local(
            target.metadata_url, path=target.path, collection_prefix=target.collection_prefix,
        )
    return ChronosQdrantStore.remote(
        target.metadata_url, url=target.url, api_key=target.api_key,
        collection_prefix=target.collection_prefix,
    )


def _workspace_config_file(root: Path, database: DatabaseConfig, vectors: QdrantConfig | None = None, *, atomic: bool = False) -> Path:
    lines = []
    if atomic:
        lines.extend(["[workspace]", f"shared_metadata_url={json.dumps(database.metadata_url or database.database_url)}"])
    lines.extend(["[stores.app]", f"kind={json.dumps(urlsplit(database.database_url).scheme)}",
                  f"database_url={json.dumps(database.database_url)}"])
    if database.metadata_url:
        lines.append(f"metadata_url={json.dumps(database.metadata_url)}")
    if vectors is not None:
        lines.extend(["[stores.vectors]", 'kind="qdrant"', f"metadata_url={json.dumps(vectors.metadata_url)}",
                      f"collection_prefix={json.dumps(vectors.collection_prefix)}"])
        if vectors.path is not None:
            lines.append(f"path={json.dumps(str(vectors.path))}")
        else:
            lines.append(f"url={json.dumps(vectors.url)}")
            if vectors.api_key:
                lines.append(f"api_key={json.dumps(vectors.api_key)}")
    config = root / "chronos.toml"
    config.write_text("\n".join(lines) + "\n")
    return config


def _invoke_workspace(config: Path, *args: str, ok: bool = True, input_text: str | None = None) -> Any:
    environment = {key: value for key, value in os.environ.items() if not key.startswith("CHRONOS_")}
    environment["CHRONOS_WORKSPACE_CONFIG"] = str(config)
    return _run_cli([*args], environment=environment, ok=ok, input_text=input_text)


def _prepare_workspace_merge(root: Path, *, atomic: bool) -> Path:
    pytest.importorskip("qdrant_client")
    database = DatabaseConfig(f"sqlite:///{root / 'app.sqlite'}")
    target = QdrantConfig(
        metadata_url=database.database_url if atomic else f"sqlite:///{root / 'vectors.sqlite'}",
        path=root / "vectors",
    )
    config = _workspace_config_file(root, database, target, atomic=atomic)
    _invoke_workspace(config, "table", "create", "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)", "--store", "app")
    _invoke_workspace(config, "table", "register", "items", "--primary-key", "id", "--store", "app")
    _invoke_workspace(config, "execute", "INSERT INTO items (id, value) VALUES (1, 'base')", "--store", "app")
    _invoke_workspace(config, "qdrant", "--store", "vectors", "collection", "register", "documents", "--dimensions", "3")
    _invoke_workspace(config, "branch", "create", "experiment")
    _invoke_workspace(config, "execute", "UPDATE items SET value = 'candidate' WHERE id = 1", "--store", "app", "--branch", "experiment")
    _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "upsert", "documents", "one", '{"vector":[1,0,0]}', "--branch", "experiment")
    return config


def _qdrant_workspace_file(root: Path, target: QdrantConfig) -> Path:
    config = _workspace_config_file(root, DatabaseConfig(target.metadata_url), target)
    # A Qdrant-only workspace still has its own relational metadata context.
    text = config.read_text()
    config.write_text(text[text.index("[stores.vectors]"):])
    return config


class TestWorkspaceResources:
    @pytest.mark.parametrize("kind", ["sqlite", "sqlite-split", "duckdb-sqlite"])
    def test_database_paths_preserve_reserved_characters(self, tmp_path: Path, kind: str) -> None:
        if kind == "duckdb-sqlite" and not _native_interval.has_duckdb:
            if os.environ.get("CHRONOS_CLI_REQUIRE_DUCKDB") == "ON":
                pytest.fail("This test run requires a DuckDB-enabled native build")
            pytest.skip("DuckDB disabled in this native build")
        root = tmp_path / ("workspace #%20+目录" if kind == "duckdb-sqlite" else "workspace #?%20+目录")
        root.mkdir()
        scheme = "duckdb" if kind == "duckdb-sqlite" else "sqlite"
        data = root / ("data%20#.duckdb" if scheme == "duckdb" else "data%20#?.sqlite")
        metadata = root / "metadata%20#?.sqlite"
        config = root / "chronos.toml"
        config.write_text(
            f'[stores.app]\nkind="{scheme}"\ndatabase_url="{scheme}:///{quote(data.name, safe="")}"\n'
            + (f'metadata_url="sqlite:///{quote(metadata.name, safe="")}"\n' if kind != "sqlite" else '')
        )
        loaded = load_workspace_config(config)
        with open_workspace(loaded) as workspace:
            app = workspace.stores["app"]
            app.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
            app.db.commit()
            app.register_table("items", ["id"])
            with app.checkout("main") as session:
                session.execute("INSERT INTO items (id, value) VALUES (1, 'persisted')")
        assert data.is_file()
        if kind != "sqlite":
            assert metadata.is_file()
        with open_workspace(loaded) as workspace:
            with workspace.stores["app"].checkout("main") as session:
                assert session.query("SELECT value FROM items") == [{"value": "persisted"}]
        assert sorted(path.name for path in tmp_path.iterdir()) == [root.name]

    def test_hash_directories_keep_workspace_state_separate(self, tmp_path: Path) -> None:
        configs = []
        for name in ("workspace#one", "workspace#two"):
            root = tmp_path / name
            root.mkdir()
            config = root / "chronos.toml"
            config.write_text('[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n')
            configs.append(config)
        _invoke_workspace(configs[0], "branch", "create", "private-to-one")
        assert _invoke_workspace(configs[1], "branch", "list") == ["main"]
        assert sorted(path.name for path in tmp_path.iterdir()) == ["workspace#one", "workspace#two"]

    def test_sqlite_workspace_without_qdrant_dependency(self, tmp_path: Path) -> None:
        config_file = tmp_path / "chronos.toml"
        config_file.write_text('[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n')
        code = '''
            import importlib.abc
            import json
            import sys
            from pathlib import Path

            class BlockQdrant(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname == "qdrant_client" or fullname.startswith("qdrant_client."):
                        raise ModuleNotFoundError("simulated missing qdrant-client")

            sys.meta_path.insert(0, BlockQdrant())
            from chronos_core.cli._config import load_workspace_config, open_workspace
            with open_workspace(load_workspace_config(Path(sys.argv[1]))) as workspace:
                print(json.dumps(workspace.list_branches()))
        '''
        result = subprocess.run(
            [sys.executable, "-X", "faulthandler", "-c", textwrap.dedent(code), str(config_file)],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == ["main"]

    def test_sqlite_workspace_persists_after_close(self, tmp_path: Path) -> None:
        config_file = tmp_path / "chronos.toml"
        config_file.write_text('[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n')
        config = load_workspace_config(config_file)
        with open_workspace(config) as workspace:
            database = workspace.stores["app"]
            database.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
            database.db.commit()
            database.register_table("items", ["id"])
            session = workspace.checkout().stores["app"]
            session.execute("INSERT INTO items (id, value) VALUES (1, 'persisted')")
        assert (tmp_path / "app.sqlite").is_file()
        with open_workspace(config) as workspace:
            assert workspace.checkout().stores["app"].query("SELECT * FROM items") == [
                {"id": 1, "value": "persisted"},
            ]

    def test_add_qdrant_to_existing_sqlite_workspace(self, tmp_path: Path) -> None:
        pytest.importorskip("qdrant_client")
        config_file = tmp_path / "chronos.toml"
        sqlite_config = '[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n'
        config_file.write_text(sqlite_config)
        with open_workspace(load_workspace_config(config_file)) as workspace:
            database = workspace.stores["app"]
            database.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
            database.db.commit()
            database.register_table("items", ["id"])
            workspace.checkout().stores["app"].execute("INSERT INTO items (id, value) VALUES (1, 'base')")
            workspace.create_branch("experiment")
            workspace.checkout("experiment").stores["app"].execute("UPDATE items SET value = 'candidate' WHERE id = 1")
        config_file.write_text(sqlite_config + '''
[workspace]
shared_metadata_url="sqlite:///app.sqlite"
[stores.vectors]
kind="qdrant"
metadata_url="sqlite:///app.sqlite"
path="vectors"
''')
        config = load_workspace_config(config_file)
        with open_workspace(config) as workspace:
            assert set(workspace.list_branches()) == {"main", "experiment"}
            workspace.stores["vectors"].register_collection("documents", 2)
            main = workspace.checkout("main")
            experiment = workspace.checkout("experiment")
            assert main.stores["app"].query("SELECT * FROM items") == [{"id": 1, "value": "base"}]
            assert experiment.stores["app"].query("SELECT * FROM items") == [{"id": 1, "value": "candidate"}]
            experiment.stores["vectors"].upsert("documents", "one", [1.0, 0.0], {"value": "candidate"})
            assert main.stores["vectors"].get("documents", "one") is None
            result = workspace.merge_atomic(
                "experiment", "main", operation_id="added-qdrant", policy="abort_on_conflict",
            )
            assert result.status == "committed"
            assert result.stores == {"app": 1, "vectors": 1}
        with open_workspace(config) as workspace:
            main = workspace.checkout("main")
            assert main.stores["app"].query("SELECT * FROM items") == [{"id": 1, "value": "candidate"}]
            assert main.stores["vectors"].get("documents", "one").payload == {"value": "candidate"}

    @pytest.mark.parametrize("atomic", [False, True])
    def test_sqlite_qdrant_workspace_reopens(self, tmp_path: Path, atomic: bool) -> None:
        pytest.importorskip("qdrant_client")
        config_file = tmp_path / "chronos.toml"
        header = '[workspace]\nshared_metadata_url="sqlite:///app.sqlite"\n' if atomic else ""
        metadata = "app.sqlite" if atomic else "vectors.sqlite"
        config_file.write_text(header + f'''
[stores.app]
kind="sqlite"
database_url="sqlite:///app.sqlite"
[stores.vectors]
kind="qdrant"
metadata_url="sqlite:///{metadata}"
path="vectors"
''')
        config = load_workspace_config(config_file)
        with open_workspace(config) as workspace:
            database = workspace.stores["app"]
            database.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
            database.db.commit()
            database.register_table("items", ["id"])
            workspace.stores["vectors"].register_collection("documents", 2)
            workspace.create_branch("experiment")
            session = workspace.checkout("experiment")
            session.stores["app"].execute("INSERT INTO items (id, value) VALUES (1, 'candidate')")
            session.stores["vectors"].upsert("documents", "one", [1.0, 0.0], {"value": "candidate"})
            workspace.create_checkpoint("saved", branch="experiment")
        assert (tmp_path / "app.sqlite").is_file()
        assert (tmp_path / "vectors").is_dir()
        with open_workspace(config) as workspace:
            main = workspace.checkout("main")
            assert main.stores["app"].query("SELECT * FROM items") == []
            assert main.stores["vectors"].get("documents", "one") is None
            assert {name: len(diff.changes) for name, diff in workspace.diff("main", "experiment").items()} == {
                "app": 1, "vectors": 1,
            }
            if atomic:
                preview = workspace.merge_atomic_preview("experiment", "main", policy="abort_on_conflict")
                result = workspace.merge_atomic(
                    "experiment", "main", policy="abort_on_conflict",
                    operation_id="workspace-persistence", preview_token=preview.preview_token,
                )
                assert result.status == "committed"
                assert result.stores == {"app": 1, "vectors": 1}
            else:
                assert set(workspace.merge_apply("experiment", "main", policy="abort_on_conflict")) == {"app", "vectors"}
        with open_workspace(config) as workspace:
            main = workspace.checkout("main")
            assert main.stores["app"].query("SELECT * FROM items") == [{"id": 1, "value": "candidate"}]
            assert main.stores["vectors"].get("documents", "one").payload == {"value": "candidate"}
            workspace.delete_branch("experiment")


class TestWorkspaceSQLWorkflows:
    def test_atomic_merge_across_separate_sql_databases(self, tmp_path: Path) -> None:
        config = tmp_path / "chronos.toml"
        config.write_text(textwrap.dedent("""
            [workspace]
            shared_metadata_url = "sqlite:///metadata.sqlite"

            [stores.app]
            kind = "sqlite"
            database_url = "sqlite:///app.sqlite"
            metadata_url = "sqlite:///metadata.sqlite"
            merge_tables = ["items"]

            [stores.audit]
            kind = "sqlite"
            database_url = "sqlite:///audit.sqlite"
            metadata_url = "sqlite:///metadata.sqlite"
            merge_tables = ["events"]
        """))
        invoke = partial(_invoke_workspace, config)
        tables = {"app": "items", "audit": "events"}
        for store, table in tables.items():
            invoke("table", "create", f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)", "--store", store)
            invoke("table", "register", table, "--store", store, "--primary-key", "id")
        invoke("branch", "create", "draft")
        for store, table in tables.items():
            invoke("execute", f"INSERT INTO {table} (id) VALUES (1)", "--store", store, "--branch", "draft")
            assert invoke("query", f"SELECT id FROM {table}", "--store", store) == []

        preview = invoke("merge", "atomic-preview", "draft", "main")
        assert {
            store: [change["table"] for change in result["changes"]]
            for store, result in preview["stores"].items()
        } == {"app": ["items"], "audit": ["events"]}
        result = invoke(
            "merge", "atomic-apply", "draft", "main", "--operation-id", "separate-sql",
            "--preview-token", preview["preview_token"],
        )
        assert result["status"] == "committed"
        assert result["stores"] == {"app": 1, "audit": 1}
        for store, table in tables.items():
            assert invoke("query", f"SELECT id FROM {table}", "--store", store) == [{"id": 1}]

    def test_nonfinite_api_result_is_not_printed_as_invalid_json(self, tmp_path: Path) -> None:
        config = _workspace_config_file(tmp_path, DatabaseConfig(f"sqlite:///{tmp_path / 'app.sqlite'}"))
        with open_workspace(load_workspace_config(config)) as workspace:
            with workspace.stores["app"].checkout("main") as session:
                assert session.query("SELECT 1e999 AS value") == [{"value": float("inf")}]
        _invoke_workspace(config, "query", "SELECT 1e999 AS value", "--store", "app", ok=False)
        assert _invoke_workspace(config, "query", "SELECT 1 AS value", "--store", "app") == [{"value": 1}]

    def test_relational_workspace(self, cli_database: DatabaseConfig, tmp_path: Path) -> None:
        config = _workspace_config_file(tmp_path, cli_database)
        _invoke_workspace(config, "table", "create", "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)", "--store", "app")
        _invoke_workspace(config, "table", "register", "items", "--primary-key", "id", "--store", "app")
        _invoke_workspace(config, "execute", "INSERT INTO items (id, value) VALUES (:id, :value)", "--store", "app", "--params", '{"id":1,"value":"base"}')
        # A second independently backed SQL store needs no special adapter in the CLI.
        audit_url = f"sqlite:///{tmp_path / 'audit.sqlite'}"
        with config.open("a") as stream:
            stream.write(f'\n[stores.audit]\nkind="sqlite"\ndatabase_url={json.dumps(audit_url)}\n')
        _invoke_workspace(config, "table", "create", "CREATE TABLE audit (id INTEGER PRIMARY KEY, value TEXT)", "--store", "audit")
        _invoke_workspace(config, "table", "register", "audit", "--primary-key", "id", "--store", "audit")
        _invoke_workspace(config, "branch", "create", "experiment")
        _invoke_workspace(config, "execute", "UPDATE items SET value = 'candidate' WHERE id = 1", "--store", "app", "--branch", "experiment")
        _invoke_workspace(config, "execute", "INSERT INTO audit (id, value) VALUES (1, 'candidate')", "--store", "audit", "--branch", "experiment")
        assert _invoke_workspace(config, "query", "SELECT value FROM items", "--store", "app") == [{"value": "base"}]
        assert _invoke_workspace(config, "query", "SELECT * FROM audit", "--store", "audit") == []
        checkpoint = _invoke_workspace(config, "checkpoint", "create", "saved", "--branch", "experiment")
        assert set(checkpoint) == {"app", "audit"}
        _invoke_workspace(config, "branch", "create-from-checkpoint", "restored", "saved")
        assert _invoke_workspace(config, "query", "SELECT value FROM items", "--store", "app", "--branch", "restored") == [{"value": "candidate"}]
        difference = _invoke_workspace(config, "diff", "main", "experiment")
        assert {name: len(result["changes"]) for name, result in difference.items()} == {"app": 1, "audit": 1}
        assert set(_invoke_workspace(config, "merge", "preview", "experiment", "main")) == {"app", "audit"}
        _invoke_workspace(config, "merge", "atomic-preview", "experiment", "main", ok=False)
        assert set(_invoke_workspace(config, "merge", "apply", "experiment", "main")) == {"app", "audit"}
        assert _invoke_workspace(config, "query", "SELECT value FROM items", "--store", "app") == [{"value": "candidate"}]
        assert _invoke_workspace(config, "query", "SELECT value FROM audit", "--store", "audit") == [{"value": "candidate"}]
        _invoke_workspace(config, "branch", "create", "conflicting")
        _invoke_workspace(config, "execute", "UPDATE items SET value = 'source' WHERE id = 1", "--store", "app", "--branch", "conflicting")
        _invoke_workspace(config, "execute", "UPDATE items SET value = 'target' WHERE id = 1", "--store", "app")
        assert len(_invoke_workspace(config, "merge", "preview", "conflicting", "main")["app"]["conflicts"]) == 1
        for policy in ("abort_on_conflict", "snapshot_isolation"):
            _invoke_workspace(config, "merge", "apply", "conflicting", "main", "--policy", policy, ok=False)
        assert _invoke_workspace(config, "query", "SELECT value FROM items", "--store", "app") == [{"value": "target"}]
        _invoke_workspace(config, "branch", "delete", "conflicting")
        _invoke_workspace(config, "query", "SELECT * FROM items", "--store", "app", "--branch", "missing", ok=False)
        _invoke_workspace(config, "execute", "DROP TABLE items", "--store", "app", ok=False)
        _invoke_workspace(config, "branch", "delete", "restored")
        _invoke_workspace(config, "branch", "delete", "experiment")
        assert _invoke_workspace(config, "branch", "list") == ["main"]

    def test_relational_sql_inputs_and_rollback(self, cli_database: DatabaseConfig, tmp_path: Path) -> None:
        config = _workspace_config_file(tmp_path, cli_database)
        invoke = partial(_invoke_workspace, config)
        statement = tmp_path / "statement.sql"
        statement.write_text(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)", encoding="utf-8",
        )
        invoke("table", "create", "--store", "app", "--file", str(statement))
        invoke("table", "register", "--store", "app", "items", "--primary-key", "id")
        statement.write_text(
            "INSERT INTO items (id, value) VALUES (:id, :value)", encoding="utf-8",
        )
        assert invoke("execute", "--store", "app", "--file", str(statement), "--params",
                      '{"id": 1, "value": "100% done"}') == {"rowcount": 1}
        assert invoke("query", "--store", "app", "--stdin", "--params", '{"pattern": "100%"}',
                      input_text="SELECT * FROM items WHERE value LIKE :pattern") == [
            {"id": 1, "value": "100% done"},
        ]
        assert invoke("execute", "--store", "app", "--stdin", "--params", '{"id": 1, "value": "updated"}',
                      input_text="UPDATE items SET value = :value WHERE id = :id") == {"rowcount": 1}
        statement.write_text("SELECT * FROM items", encoding="utf-8")
        assert invoke("query", "--store", "app", "--file", str(statement)) == [{"id": 1, "value": "updated"}]
        # A failed statement must not leave its earlier rows committed.
        invoke("execute", "--store", "app", "INSERT INTO items (id, value) VALUES (2, 'partial'), (1, 'duplicate')", ok=False)
        assert invoke("query", "--store", "app", input_text="SELECT * FROM items ORDER BY id") == [
            {"id": 1, "value": "updated"},
        ]

    def test_relational_aggregation(self, cli_database: DatabaseConfig, tmp_path: Path) -> None:
        config = _workspace_config_file(tmp_path, cli_database)
        invoke = partial(_invoke_workspace, config)
        invoke("table", "create", "--store", "app", "CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount BIGINT)")
        invoke("table", "register", "--store", "app", "sales", "--primary-key", "id")
        invoke("execute", "--store", "app", "INSERT INTO sales (id, category, amount) VALUES (1, 'books', 10), (2, 'books', 20), (3, 'games', 50)")
        invoke("branch", "create", "forecast")
        invoke("execute", "--store", "app", "UPDATE sales SET amount = amount * :factor WHERE category = :category",
               "--branch", "forecast", "--params", '{"factor": 2, "category": "books"}')
        query = "SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY category"
        # PostgreSQL SUM(BIGINT) returns NUMERIC, serialized as a precision-preserving string.
        total = str if urlsplit(cli_database.database_url).scheme in {"postgres", "postgresql"} else int
        assert invoke("query", "--store", "app", query) == [
            {"category": "books", "total": total(30)}, {"category": "games", "total": total(50)},
        ]
        assert invoke("query", "--store", "app", query, "--branch", "forecast") == [
            {"category": "books", "total": total(60)}, {"category": "games", "total": total(50)},
        ]


class TestWorkspaceQdrantWorkflows:
    def test_duckdb_and_qdrant(self, tmp_path: Path) -> None:
        if not _native_interval.has_duckdb:
            if os.environ.get("CHRONOS_CLI_REQUIRE_DUCKDB") == "ON":
                pytest.fail("The CLI test matrix requires a DuckDB-enabled native build")
            pytest.skip("DuckDB disabled in this native build")
        pytest.importorskip("qdrant_client")
        metadata = f"sqlite:///{tmp_path / 'metadata.sqlite'}"
        database = DatabaseConfig(f"duckdb:///{tmp_path / 'data.duckdb'}", metadata)
        vectors = QdrantConfig(metadata, path=tmp_path / "vectors")
        config = _workspace_config_file(tmp_path, database, vectors, atomic=True)
        _invoke_workspace(config, "table", "create", "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)", "--store", "app")
        _invoke_workspace(config, "table", "register", "items", "--primary-key", "id", "--store", "app")
        _invoke_workspace(config, "qdrant", "--store", "vectors", "collection", "register", "documents", "--dimensions", "3")
        _invoke_workspace(config, "branch", "create", "experiment")
        _invoke_workspace(config, "execute", "INSERT INTO items (id, value) VALUES (1, 'candidate')", "--store", "app", "--branch", "experiment")
        _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "upsert", "documents", "one", '{"vector":[1,0,0]}', "--branch", "experiment")
        assert _invoke_workspace(config, "query", "SELECT * FROM items", "--store", "app") == []
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "get", "documents", "one") is None
        preview = _invoke_workspace(config, "merge", "atomic-preview", "experiment", "main")
        assert {name: len(result["changes"]) for name, result in preview["stores"].items()} == {"app": 1, "vectors": 1}
        result = _invoke_workspace(config, "merge", "atomic-apply", "experiment", "main", "--operation-id", "duckdb-qdrant", "--preview-token", preview["preview_token"])
        assert result["status"] == "committed"
        assert result["stores"] == {"app": 1, "vectors": 1}
        assert _invoke_workspace(config, "query", "SELECT * FROM items", "--store", "app") == [{"id": 1, "value": "candidate"}]
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "get", "documents", "one")["vector"] == [1, 0, 0]

    @pytest.mark.parametrize("atomic", [False, True])
    def test_two_store_workflow(self, qdrant_target: QdrantConfig, tmp_path: Path, atomic: bool) -> None:
        target = qdrant_target
        database = DatabaseConfig(target.metadata_url if atomic else f"sqlite:///{tmp_path / 'app.sqlite'}")
        # Begin with SQLite/PostgreSQL alone, then add Qdrant to existing data.
        config = _workspace_config_file(tmp_path, database)
        _invoke_workspace(config, "table", "create", "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)", "--store", "app")
        _invoke_workspace(config, "table", "register", "items", "--primary-key", "id", "--store", "app")
        _invoke_workspace(config, "execute", "INSERT INTO items (id, value) VALUES (1, 'base')", "--store", "app")
        config = _workspace_config_file(tmp_path, database, target, atomic=atomic)
        _invoke_workspace(config, "qdrant", "--store", "vectors", "collection", "register", "documents", "--dimensions", "3")
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "collection", "info", "documents")["dimensions"] == 3
        _invoke_workspace(config, "branch", "create", "experiment")
        _invoke_workspace(config, "execute", "UPDATE items SET value = 'candidate' WHERE id = 1", "--store", "app", "--branch", "experiment")
        _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "upsert", "documents", "one", "--stdin", "--branch", "experiment", input_text='{"vector":[1,0,0],"payload":{"value":"candidate"}}')
        assert _invoke_workspace(config, "query", "--store", "app", "--stdin", input_text="SELECT value FROM items") == [{"value": "base"}]
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "get", "documents", "one") is None
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "search", "documents", "[1,0,0]", "--branch", "experiment")[0]["id"] == "one"
        _invoke_workspace(config, "checkpoint", "create", "saved", "--branch", "experiment")
        _invoke_workspace(config, "branch", "create-from-checkpoint", "restored", "saved")
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "get", "documents", "one", "--branch", "restored")["payload"] == {"value": "candidate"}
        assert {name: len(result["changes"]) for name, result in _invoke_workspace(config, "diff", "main", "experiment").items()} == {"app": 1, "vectors": 1}
        if atomic:
            preview = _invoke_workspace(config, "merge", "atomic-preview", "experiment", "main")
            _invoke_workspace(config, "merge", "apply", "experiment", "main", ok=False)
            result = _invoke_workspace(config, "merge", "atomic-apply", "experiment", "main", "--operation-id", "workspace-demo", "--preview-token", preview["preview_token"])
            assert result["status"] == "committed"
            assert result["stores"] == {"app": 1, "vectors": 1}
        else:
            assert set(_invoke_workspace(config, "merge", "preview", "experiment", "main")) == {"app", "vectors"}
            _invoke_workspace(config, "merge", "apply", "experiment", "main")
        assert _invoke_workspace(config, "query", "SELECT value FROM items", "--store", "app") == [{"value": "candidate"}]
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "get", "documents", "one")["payload"] == {"value": "candidate"}
        _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "delete", "documents", "one")
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "list", "documents") == []
        _invoke_workspace(config, "branch", "delete", "restored")
        _invoke_workspace(config, "branch", "delete", "experiment")
        assert _invoke_workspace(config, "branch", "list") == ["main"]

    def test_qdrant_cli_conflicts_and_failed_operations(self, qdrant_target: QdrantConfig, tmp_path: Path) -> None:
        config = _qdrant_workspace_file(tmp_path, qdrant_target)
        vectors = partial(_invoke_workspace, config, "qdrant", "--store", "vectors")
        workspace = partial(_invoke_workspace, config)
        vectors("collection", "register", "docs", "--dimensions", "3")
        vectors("point", "upsert", "docs", "one", '{"vector":[1,0,0],"payload":{"value":"base"}}')
        workspace("branch", "create", "trial")
        workspace("branch", "create", "trial", ok=False)
        vectors("point", "upsert", "docs", "one", '{"vector":[0,1,0],"payload":{"value":"trial"}}', "--branch", "trial")
        vectors("point", "upsert", "docs", "one", '{"vector":[0,0,1],"payload":{"value":"main"}}')
        assert len(workspace("merge", "preview", "trial", "main")["vectors"]["conflicts"]) == 1
        workspace("merge", "apply", "trial", "main", ok=False)
        workspace("merge", "apply", "trial", "main", "--policy", "snapshot_isolation", ok=False)
        vectors("point", "upsert", "docs", "bad", '{"vector":[1,0]}', ok=False)
        assert vectors("point", "get", "docs", "bad") is None
        assert vectors("point", "get", "docs", "one")["payload"] == {"value": "main"}
        vectors("point", "get", "docs", "one", "--branch", "missing", ok=False)
        vectors("collection", "info", "missing", ok=False)

    def test_qdrant_cli_json_files_and_stdin(self, qdrant_target: QdrantConfig, tmp_path: Path) -> None:
        config = _qdrant_workspace_file(tmp_path, qdrant_target)
        vectors = partial(_invoke_workspace, config, "qdrant", "--store", "vectors")
        workspace = partial(_invoke_workspace, config)
        vectors("collection", "register", "docs", "--dimensions", "3")
        data = tmp_path / "point.json"
        data.write_text('{"vector":[1,0,0],"payload":{"source":"file"}}', encoding="utf-8")
        result = vectors("point", "upsert", "docs", "one", "--file", str(data), "--revision", "initial")
        assert result["payload"] == {"source": "file"}
        store = _open_qdrant_api(qdrant_target)
        try:
            with store.context.checkout("main") as control:
                session = store.checkout_control(control)
                assert result == asdict(session.get("docs", "one"))
                expected_search = [asdict(item) for item in session.search("docs", [1, 0, 0], limit=1)]
        finally:
            store.close()
        assert vectors("search", "docs", "[1,0,0]", "--limit", "1") == expected_search
        assert vectors("search", "docs", "--stdin", input_text="[1,0,0]")[0]["id"] == "one"
        vectors("point", "upsert", "docs", "two", "--stdin", input_text='{"vector":[0,1,0]}')
        data.write_text("[0,1,0]", encoding="utf-8")
        assert vectors("search", "docs", "--file", str(data))[0]["id"] == "two"
        assert vectors("search", "docs", input_text="[1,0,0]")[0]["id"] == "one"


class TestWorkspaceFailureWorkflows:
    def test_atomic_stale_preview_is_rejected(self, tmp_path: Path) -> None:
        config = _prepare_workspace_merge(tmp_path, atomic=True)
        preview = _invoke_workspace(config, "merge", "atomic-preview", "experiment", "main")
        _invoke_workspace(config, "execute", "UPDATE items SET value = 'changed after preview' WHERE id = 1", "--store", "app", "--branch", "experiment")
        _invoke_workspace(config, "merge", "atomic-apply", "experiment", "main", "--operation-id", "stale", "--preview-token", preview["preview_token"], ok=False)
        assert _invoke_workspace(config, "query", "SELECT value FROM items", "--store", "app") == [{"value": "base"}]
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "get", "documents", "one") is None

    @pytest.mark.parametrize("atomic", [False, True])
    def test_merge_failure_retains_api_guarantees(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, atomic: bool) -> None:
        config = _prepare_workspace_merge(tmp_path, atomic=atomic)
        from chronos_core.cli.main import app
        from chronos_core.workspace.qdrant import ChronosQdrantStore

        def fail_vector_store(*args, **kwargs):
            raise RuntimeError("injected vector failure")

        # Fail the second participant after SQL has applied/staged its changes.
        method = "stage_branch_transaction_changes" if atomic else "merge_apply"
        monkeypatch.setattr(ChronosQdrantStore, method, fail_vector_store)
        command = "atomic-apply" if atomic else "apply"
        arguments = ["--config", str(config), "merge", command, "experiment", "main"]
        if atomic:
            arguments.extend(["--operation-id", "failed-merge"])
        result = CliRunner().invoke(app, arguments)
        assert result.exit_code == 1, result.output
        assert result.stdout == ""
        assert "injected vector failure" in result.stderr
        assert "Traceback" not in result.stderr
        # Atomic mode never publishes the staged SQL change; coordinated mode
        # preserves the API's partial apply instead of pretending to roll it back.
        expected = "base" if atomic else "candidate"
        assert _invoke_workspace(config, "query", "SELECT value FROM items", "--store", "app") == [{"value": expected}]
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "get", "documents", "one") is None
        assert _invoke_workspace(config, "query", "SELECT value FROM items", "--store", "app", "--branch", "experiment") == [{"value": "candidate"}]

    def test_conflict_blocks_atomic_merge(self, tmp_path: Path) -> None:
        config = _prepare_workspace_merge(tmp_path, atomic=True)
        _invoke_workspace(config, "execute", "UPDATE items SET value = 'main change' WHERE id = 1", "--store", "app")
        preview = _invoke_workspace(config, "merge", "atomic-preview", "experiment", "main")
        assert len(preview["stores"]["app"]["conflicts"]) == 1
        _invoke_workspace(config, "merge", "atomic-apply", "experiment", "main", "--operation-id", "conflicting", ok=False)
        assert _invoke_workspace(config, "query", "SELECT value FROM items", "--store", "app") == [{"value": "main change"}]
        assert _invoke_workspace(config, "qdrant", "--store", "vectors", "point", "get", "documents", "one") is None


class TestWorkspaceFilesystemWorkflows:
    @pytest.mark.parametrize('with_sql', [False, True])
    def test_persistent_workspace(self, tmp_path: Path, with_sql: bool) -> None:
        import shutil

        available = (sys.platform == 'linux' and shutil.which('fuse-overlayfs')
                     and (shutil.which('fusermount3') or shutil.which('fusermount'))
                     and Path('/dev/fuse').exists())
        if not available:
            if os.environ.get('CHRONOS_CLI_REQUIRE_FILESYSTEM') == 'ON':
                pytest.fail('Filesystem integration requires Linux, /dev/fuse, and fuse-overlayfs')
            pytest.skip('Filesystem integration requires Linux FUSE')
        base = tmp_path / 'base'
        base.mkdir()
        (base / 'report.txt').write_text('original')
        config = tmp_path / 'chronos.toml'
        config.write_text('[filesystem]\nroot="base"\nstate_dir="state"\n' + (
            '[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n' if with_sql else ''
        ))

        def run(branch: str, code: str) -> dict[str, Any]:
            return _invoke_workspace(config, 'fs', 'run', '--branch', branch, '--json', '--', sys.executable, '-c', code)

        assert _invoke_workspace(config, 'branch', 'list') == ['main']
        if with_sql:
            _invoke_workspace(config, 'table', 'create', '--store', 'app', 'CREATE TABLE docs (id INTEGER PRIMARY KEY)')
            _invoke_workspace(config, 'table', 'register', '--store', 'app', 'docs', '--primary-key', 'id')
        _invoke_workspace(config, 'branch', 'create', 'feature')
        run('feature', 'from pathlib import Path; Path("report.txt").write_text("changed")')
        assert run('main', 'print(open("report.txt").read())')['stdout'] == 'original\n'
        assert run('feature', 'print(open("report.txt").read())')['stdout'] == 'changed\n'
        if with_sql:
            _invoke_workspace(config, 'execute', '--store', 'app', '--branch', 'feature', 'INSERT INTO docs (id) VALUES (1)')
        checkpoint = _invoke_workspace(config, 'checkpoint', 'create', 'saved', '--branch', 'feature')
        assert 'filesystem' in checkpoint
        _invoke_workspace(config, 'branch', 'create-from-checkpoint', 'restored', 'saved')
        assert run('restored', 'print(open("report.txt").read())')['stdout'] == 'changed\n'
        diff = _invoke_workspace(config, 'diff', 'main', 'feature')
        with open_workspace(load_workspace_config(config)) as workspace:
            assert diff == json.loads(json.dumps({key: asdict(value) for key, value in workspace.diff('main', 'feature').items()}))
        assert diff['filesystem']['changes'][0]['path'] == 'report.txt'
        # Preserve the API's refusal of policy-aware filesystem merging.
        _invoke_workspace(config, 'merge', 'preview', 'feature', 'main', ok=False)
        _invoke_workspace(config, 'merge', 'apply', 'feature', 'main', ok=False)
        preview = _invoke_workspace(config, 'merge', 'preview', 'feature', 'main', '--policy', 'api-default')
        assert preview['filesystem'] == diff['filesystem']
        merged = _invoke_workspace(config, 'merge', 'apply', 'feature', 'main', '--policy', 'api-default')
        assert merged['filesystem']['applied'] == 1
        assert run('main', 'print(open("report.txt").read())')['stdout'] == 'changed\n'
        if with_sql:
            assert _invoke_workspace(config, 'query', '--store', 'app', 'SELECT * FROM docs') == [{'id': 1}]
        _invoke_workspace(config, 'branch', 'delete', 'restored')
        _invoke_workspace(config, 'branch', 'delete', 'feature')
        assert _invoke_workspace(config, 'branch', 'list') == ['main']
        assert (base / 'report.txt').read_text() == 'original'
        assert (tmp_path / 'state' / 'metadata.json').exists()
        assert str(tmp_path / 'state' / 'mounts') not in Path('/proc/self/mountinfo').read_text()

        environment = {key: value for key, value in os.environ.items() if not key.startswith('CHRONOS_')}
        command = [sys.executable, '-m', 'chronos_core.cli', '--config', str(config), 'fs', 'run']
        failed = subprocess.run([*command, '--', sys.executable, '-c',
                                 'import sys; print("out"); print("err", file=sys.stderr); sys.exit(7)'],
                                capture_output=True, text=True, env=environment, timeout=30)
        assert (failed.returncode, failed.stdout, failed.stderr) == (7, 'out\n', 'err\n')
        for args in [ ['--cwd', '..', '--', 'true'], ['--timeout', '0.01', '--', sys.executable, '-c', 'import time; time.sleep(1)'] ]:
            failed = subprocess.run([*command, *args], capture_output=True, text=True, env=environment, timeout=30)
            assert failed.returncode == 1
            assert 'Traceback' not in failed.stderr
            assert str(tmp_path / 'state' / 'mounts') not in Path('/proc/self/mountinfo').read_text()


class TestNativeChronosFSWorkflows:
    @pytest.fixture(autouse=True)
    def require_native_filesystem(self) -> None:
        if not hasattr(_native_interval, "NativeChronosFSStore"):
            if os.environ.get("CHRONOS_CLI_REQUIRE_CHRONOSFS") == "ON":
                pytest.fail("This test run requires a native ChronosFS build")
            pytest.skip("Native ChronosFS is disabled in this build")

    @staticmethod
    def file_command(config: Path, *args: str, payload: bytes | None = None) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            [sys.executable, "-m", "chronos_core.cli", "--config", str(config), "fs", *args],
            input=payload, capture_output=True, timeout=30,
        )

    def test_native_only_persists_binary_file(self, tmp_path: Path) -> None:
        config = tmp_path / "chronos.toml"
        config.write_text('[filesystem]\nkind="chronosfs"\ndatabase_url="sqlite:///files.sqlite"\n')
        payload = b"\x00\xff\n"
        written = self.file_command(config, "write", "/dir/file", "--stdin", "--parents", payload=payload)
        assert written.returncode == 0, written.stderr
        assert _invoke_workspace(config, "fs", "ls", "/dir") == ["file"]
        read = self.file_command(config, "read", "/dir/file")
        assert read.returncode == 0, read.stderr
        assert read.stdout == payload

    @pytest.mark.parametrize("split", [False, True], ids=["unified", "split-scoped"])
    def test_sql_and_native_filesystem_atomic_merge(self, tmp_path: Path, split: bool) -> None:
        config = tmp_path / "chronos.toml"
        sql_config = '[stores.app]\nkind="sqlite"\ndatabase_url="sqlite:///app.sqlite"\n'
        fs_config = '[filesystem]\nkind="chronosfs"\n'
        if split:
            sql_config += 'merge_tables=["items"]\n'
            fs_config += 'database_url="sqlite:///files.sqlite"\nmetadata_url="sqlite:///app.sqlite"\n'
        else:
            fs_config += 'database_url="sqlite:///app.sqlite"\n'
        config.write_text(
            '[workspace]\nshared_metadata_url="sqlite:///app.sqlite"\n' + sql_config + fs_config
        )
        invoke = partial(_invoke_workspace, config)
        invoke("table", "create", "CREATE TABLE items (id INTEGER PRIMARY KEY)", "--store", "app")
        invoke("table", "register", "items", "--primary-key", "id", "--store", "app")
        invoke("branch", "create", "draft")
        invoke("execute", "INSERT INTO items (id) VALUES (1)", "--store", "app", "--branch", "draft")
        written = self.file_command(config, "write", "/report", "--branch", "draft", "--stdin", payload=b"draft\n")
        assert written.returncode == 0, written.stderr
        assert invoke("query", "SELECT * FROM items", "--store", "app") == []
        assert "report" not in invoke("fs", "ls")
        preview = invoke("merge", "atomic-preview", "draft", "main")
        assert all(preview["stores"][name]["changes"] for name in ("app", "filesystem"))
        if split:
            assert [change["table"] for change in preview["stores"]["app"]["changes"]] == ["items"]
        result = invoke("merge", "atomic-apply", "draft", "main",
                        "--operation-id", "native-filesystem", "--preview-token", preview["preview_token"])
        assert result["status"] == "committed"
        assert invoke("query", "SELECT * FROM items", "--store", "app") == [{"id": 1}]
        read = self.file_command(config, "read", "/report")
        assert read.returncode == 0, read.stderr
        assert read.stdout == b"draft\n"

    @pytest.mark.parametrize("directory", ["plain", "workspace #?%20+目录"])
    def test_mount_writes_to_selected_branch(self, tmp_path: Path, directory: str) -> None:
        import shutil

        unmount = shutil.which("fusermount3")
        if unmount is None or not Path("/dev/fuse").exists():
            if os.environ.get("CHRONOS_CLI_REQUIRE_CHRONOSFS") == "ON":
                pytest.fail("This test requires /dev/fuse and fusermount3")
            pytest.skip("Native mount requires /dev/fuse and fusermount3")

        tmp_path = tmp_path / directory
        tmp_path.mkdir()
        config = tmp_path / "chronos.toml"
        config.write_text('[filesystem]\nkind="chronosfs"\ndatabase_url="sqlite:///files.sqlite"\n')
        mountpoint = tmp_path / "mount"
        mountpoint.mkdir()
        _invoke_workspace(config, "branch", "create", "draft")
        command = [
            sys.executable, "-m", "chronos_core.cli", "--config", str(config),
            "fs", "mount", str(mountpoint), "--branch", "draft",
        ]
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            try:
                deadline = time.monotonic() + 15
                while not mountpoint.is_mount():
                    if process.poll() is not None:
                        _, stderr = process.communicate()
                        pytest.fail(f"Mount command exited early: {stderr.decode(errors='replace')}")
                    if time.monotonic() >= deadline:
                        pytest.fail("Timed out waiting for the native mount")
                    time.sleep(0.05)
                payload = b"mounted draft\n" * 8192
                (mountpoint / "report").write_bytes(payload)
                subprocess.run([unmount, "-u", str(mountpoint)], check=True, timeout=10)
                _, stderr = process.communicate(timeout=15)
                assert process.returncode == 0, stderr
            finally:
                if mountpoint.is_mount():
                    subprocess.run([unmount, "-uz", str(mountpoint)], timeout=10)
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)

        assert "report" not in _invoke_workspace(config, "fs", "ls")
        read = self.file_command(config, "read", "/report", "--branch", "draft")
        assert read.returncode == 0, read.stderr
        assert read.stdout == payload
        assert (tmp_path / "files.sqlite.chronosfs-objects").is_dir()
