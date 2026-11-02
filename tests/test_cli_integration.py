"""Real CLI workflows across independent relational and vector stores."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import asdict
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql as postgres_sql

from chronos_core import _native_interval
from chronos_core.cli._qdrant import QdrantConfig
from chronos_core.cli._relational import DatabaseConfig


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


def _invoke_sql(database: DatabaseConfig, *args: str,
                ok: bool = True, input_text: str | None = None) -> Any:
    arguments = ["--database-url", database.database_url]
    if database.metadata_url is not None:
        arguments.extend(["--metadata-url", database.metadata_url])
    environment = dict(os.environ)
    environment.pop("CHRONOS_METADATA_URL", None)
    return _run_cli([*arguments, *args], environment=environment, ok=ok, input_text=input_text)


def _invoke_qdrant(target: QdrantConfig, *args: str, ok: bool = True, input_text: str | None = None):
    env = {key: value for key, value in os.environ.items() if not key.startswith("CHRONOS_")}
    env["CHRONOS_METADATA_URL"] = target.metadata_url
    env["CHRONOS_QDRANT_COLLECTION_PREFIX"] = target.collection_prefix
    if target.path is not None:
        env["CHRONOS_QDRANT_PATH"] = str(target.path)
    else:
        env["CHRONOS_QDRANT_URL"] = target.url
        if target.api_key is not None:
            env["CHRONOS_QDRANT_API_KEY"] = target.api_key
    return _run_cli(["qdrant", *args], environment=env, ok=ok, input_text=input_text)


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


class TestRelationalWorkflows:
    def test_relational_workflow(self, cli_database: DatabaseConfig) -> None:
        invoke = partial(_invoke_sql, cli_database)
        invoke("table", "create", "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
        assert invoke("table", "register", "items", "--primary-key", "id") is None
        assert invoke("branch", "create", "trial") is None
        invoke("branch", "create", "trial", ok=False)
        assert {branch["branch_id"] for branch in invoke("branch", "list")} == {"main", "trial"}
        assert invoke("execute", "INSERT INTO items (id, value) VALUES (:id, :value)", "--branch", "trial",
                      "--params", '{"id": 1, "value": "hello"}') == {"rowcount": 1}
        assert invoke("query", "SELECT * FROM items", "--branch", "trial") == [{"id": 1, "value": "hello"}]
        assert invoke("query", "SELECT * FROM items") == []
        assert invoke("checkpoint", "create", "saved", "--branch", "trial")["checkpoint_id"] == "saved"
        assert len(invoke("checkpoint", "list", "--branch", "trial")) == 1
        assert len(invoke("diff", "main", "trial")["changes"]) == 1
        assert len(invoke("diff", "main", "trial", "--table", "items")) == 1
        assert invoke("merge", "preview", "trial", "main")["conflicts"] == []
        assert invoke("merge", "apply", "trial", "main")["applied"] == 1
        assert invoke("query", "SELECT * FROM items") == [{"id": 1, "value": "hello"}]
        invoke("branch", "create", "conflicting")
        invoke("execute", "UPDATE items SET value = 'source' WHERE id = 1", "--branch", "conflicting")
        invoke("execute", "UPDATE items SET value = 'target' WHERE id = 1")
        assert len(invoke("merge", "preview", "conflicting", "main")["conflicts"]) == 1
        invoke("merge", "apply", "conflicting", "main", ok=False)
        invoke("merge", "apply", "conflicting", "main", "--policy", "snapshot_isolation", ok=False)
        assert invoke("query", "SELECT value FROM items") == [{"value": "target"}]
        invoke("branch", "delete", "conflicting")
        invoke("query", "SELECT * FROM items", "--branch", "missing", ok=False)
        invoke("execute", "DROP TABLE items", ok=False)
        assert invoke("branch", "delete", "trial") is None
        assert [branch["branch_id"] for branch in invoke("branch", "list")] == ["main"]

    def test_relational_sql_inputs_and_rollback(self, cli_database: DatabaseConfig, tmp_path: Path) -> None:
        invoke = partial(_invoke_sql, cli_database)
        statement = tmp_path / "statement.sql"
        statement.write_text(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)", encoding="utf-8",
        )
        invoke("table", "create", "--file", str(statement))
        invoke("table", "register", "items", "--primary-key", "id")
        statement.write_text(
            "INSERT INTO items (id, value) VALUES (:id, :value)", encoding="utf-8",
        )
        assert invoke("execute", "--file", str(statement), "--params",
                      '{"id": 1, "value": "100% done"}') == {"rowcount": 1}
        assert invoke("query", "--stdin", "--params", '{"pattern": "100%"}',
                      input_text="SELECT * FROM items WHERE value LIKE :pattern") == [
            {"id": 1, "value": "100% done"},
        ]
        assert invoke("execute", "--stdin", "--params", '{"id": 1, "value": "updated"}',
                      input_text="UPDATE items SET value = :value WHERE id = :id") == {"rowcount": 1}
        statement.write_text("SELECT * FROM items", encoding="utf-8")
        assert invoke("query", "--file", str(statement)) == [{"id": 1, "value": "updated"}]
        # A failed statement must not leave its earlier rows committed.
        invoke("execute", "INSERT INTO items (id, value) VALUES (2, 'partial'), (1, 'duplicate')", ok=False)
        assert invoke("query", input_text="SELECT * FROM items ORDER BY id") == [
            {"id": 1, "value": "updated"},
        ]

    def test_relational_aggregation(self, cli_database: DatabaseConfig) -> None:
        invoke = partial(_invoke_sql, cli_database)
        invoke("table", "create", "CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount BIGINT)")
        invoke("table", "register", "sales", "--primary-key", "id")
        invoke("execute", "INSERT INTO sales (id, category, amount) VALUES (1, 'books', 10), (2, 'books', 20), (3, 'games', 50)")
        invoke("branch", "create", "forecast")
        invoke("execute", "UPDATE sales SET amount = amount * :factor WHERE category = :category",
               "--branch", "forecast", "--params", '{"factor": 2, "category": "books"}')
        query = "SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY category"
        # PostgreSQL SUM(BIGINT) returns NUMERIC, serialized as a precision-preserving string.
        total = str if urlsplit(cli_database.database_url).scheme in {"postgres", "postgresql"} else int
        assert invoke("query", query) == [
            {"category": "books", "total": total(30)}, {"category": "games", "total": total(50)},
        ]
        assert invoke("query", query, "--branch", "forecast") == [
            {"category": "books", "total": total(60)}, {"category": "games", "total": total(50)},
        ]


class TestQdrantWorkflows:
    def test_qdrant_cli_workflow(self, qdrant_target: QdrantConfig) -> None:
        target = qdrant_target
        info = _invoke_qdrant(target, "collection", "register", "documents", "--dimensions", "3")
        assert info["name"] == "documents"
        assert info["dimensions"] == 3
        assert _invoke_qdrant(target, "collection", "info", "documents") == info
        assert _invoke_qdrant(target, "collection", "list") == [info]
        first = {"vector": [1, 0, 0], "payload": {"title": "Runtime"}}
        second = {"vector": [0, 1, 0], "payload": {"title": "Billing"}}
        point = _invoke_qdrant(target, "point", "upsert", "documents", "runtime", json.dumps(first))
        _invoke_qdrant(target, "point", "upsert", "documents", "billing", json.dumps(second))
        assert _invoke_qdrant(target, "point", "get", "documents", "runtime") == point
        assert {item["id"] for item in _invoke_qdrant(target, "point", "list", "documents")} == {"runtime", "billing"}
        assert _invoke_qdrant(target, "point", "get", "documents", "missing") is None
        results = _invoke_qdrant(target, "search", "documents", "[1,0,0]", "--limit", "2")
        assert [item["id"] for item in results] == ["runtime", "billing"]
        assert results[0]["score"] == pytest.approx(1.0, abs=1e-6)
        assert results[1]["score"] == pytest.approx(0.0, abs=1e-6)
        assert len(_invoke_qdrant(target, "search", "documents", "[1,0,0]", "--score-threshold", "0.9")) == 1

        # Independent expected data plus a direct public-API comparison.
        store = _open_qdrant_api(target)
        try:
            with store.context.checkout("main") as control:
                session = store.checkout_control(control)
                assert point == asdict(session.get("documents", "runtime"))
                assert results == [asdict(item) for item in session.search("documents", [1, 0, 0], limit=2)]
        finally:
            store.close()

        _invoke_qdrant(target, "branch", "create", "experiment")
        assert {item["branch_id"] for item in _invoke_qdrant(target, "branch", "list")} == {"main", "experiment"}
        changed = {"vector": [0, 0, 1], "payload": {"title": "Experiment"}}
        _invoke_qdrant(target, "point", "upsert", "documents", "runtime", json.dumps(changed), "--branch", "experiment")
        assert _invoke_qdrant(target, "point", "delete", "documents", "billing", "--branch", "experiment") is True
        assert _invoke_qdrant(target, "point", "get", "documents", "billing", "--branch", "experiment") is None
        assert _invoke_qdrant(target, "point", "get", "documents", "billing")["payload"] == second["payload"]
        assert _invoke_qdrant(target, "point", "get", "documents", "runtime")["payload"] == first["payload"]
        assert _invoke_qdrant(target, "checkpoint", "create", "saved", "--branch", "experiment")["checkpoint_id"] == "saved"
        assert len(_invoke_qdrant(target, "checkpoint", "list", "--branch", "experiment")) == 1
        changes = _invoke_qdrant(target, "diff", "main", "experiment")["changes"]
        assert {(item["key"]["id"], item["change"]) for item in changes} == {("runtime", "modified"), ("billing", "deleted")}
        assert _invoke_qdrant(target, "merge", "preview", "experiment", "main")["conflicts"] == []
        assert _invoke_qdrant(target, "merge", "apply", "experiment", "main")["applied"] == 2
        assert _invoke_qdrant(target, "point", "get", "documents", "runtime")["payload"] == changed["payload"]
        assert _invoke_qdrant(target, "point", "get", "documents", "billing") is None
        assert _invoke_qdrant(target, "point", "delete", "documents", "missing") is False
        _invoke_qdrant(target, "branch", "delete", "experiment")

    def test_qdrant_cli_conflicts_and_failed_operations(self, qdrant_target: QdrantConfig) -> None:
        target = qdrant_target
        _invoke_qdrant(target, "collection", "register", "docs", "--dimensions", "3")
        _invoke_qdrant(target, "point", "upsert", "docs", "one", '{"vector":[1,0,0],"payload":{"value":"base"}}')
        _invoke_qdrant(target, "branch", "create", "trial")
        _invoke_qdrant(target, "branch", "create", "trial", ok=False)
        _invoke_qdrant(target, "point", "upsert", "docs", "one", '{"vector":[0,1,0],"payload":{"value":"trial"}}', "--branch", "trial")
        _invoke_qdrant(target, "point", "upsert", "docs", "one", '{"vector":[0,0,1],"payload":{"value":"main"}}')
        assert len(_invoke_qdrant(target, "merge", "preview", "trial", "main")["conflicts"]) == 1
        _invoke_qdrant(target, "merge", "apply", "trial", "main", ok=False)
        _invoke_qdrant(target, "merge", "apply", "trial", "main", "--policy", "snapshot_isolation", ok=False)
        _invoke_qdrant(target, "point", "upsert", "docs", "bad", '{"vector":[1,0]}', ok=False)
        assert _invoke_qdrant(target, "point", "get", "docs", "bad") is None
        assert _invoke_qdrant(target, "point", "get", "docs", "one")["payload"] == {"value": "main"}
        _invoke_qdrant(target, "point", "get", "docs", "one", "--branch", "missing", ok=False)
        _invoke_qdrant(target, "collection", "info", "missing", ok=False)

    def test_qdrant_cli_json_files_and_stdin(self, qdrant_target: QdrantConfig, tmp_path: Path) -> None:
        target = qdrant_target
        _invoke_qdrant(target, "collection", "register", "docs", "--dimensions", "3")
        data = tmp_path / "point.json"
        data.write_text('{"vector":[1,0,0],"payload":{"source":"file"}}', encoding="utf-8")
        result = _invoke_qdrant(target, "point", "upsert", "docs", "one", "--file", str(data), "--revision", "initial")
        assert result["payload"] == {"source": "file"}
        assert _invoke_qdrant(target, "search", "docs", "--stdin", input_text="[1,0,0]")[0]["id"] == "one"
        _invoke_qdrant(target, "point", "upsert", "docs", "two", "--stdin", input_text='{"vector":[0,1,0]}')
        data.write_text("[0,1,0]", encoding="utf-8")
        assert _invoke_qdrant(target, "search", "docs", "--file", str(data))[0]["id"] == "two"
        assert _invoke_qdrant(target, "search", "docs", input_text="[1,0,0]")[0]["id"] == "one"
