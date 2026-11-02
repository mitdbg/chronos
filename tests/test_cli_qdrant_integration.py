"""Persistent local and server-backed Qdrant through separate CLI processes."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import psycopg
import pytest

pytest.importorskip("qdrant_client")
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

from chronos_core.cli._qdrant_support import QdrantConfig
from chronos_core.workspace.qdrant import ChronosQdrantStore
# Reuse the existing isolated, session-owned PostgreSQL database fixture.
from tests.test_cli_integration import postgres_cli_database_url  # noqa: F401


@pytest.fixture(params=["local-sqlite", "remote-sqlite", "local-postgres", "remote-postgres"])
def qdrant_target(request: pytest.FixtureRequest):
    kind = request.param
    remote_url = os.environ.get("CHRONOS_CLI_TEST_QDRANT_URL")
    api_key = os.environ.get("CHRONOS_CLI_TEST_QDRANT_API_KEY")
    if kind.startswith("remote") and not remote_url:
        pytest.skip("Set CHRONOS_CLI_TEST_QDRANT_URL to test the Qdrant server")
    client = None
    prefix = f"cli_test_{uuid4().hex}_"
    if kind.startswith("remote"):
        client = QdrantClient(url=remote_url, api_key=api_key, timeout=2)
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


def invoke(target: QdrantConfig, *args: str, ok: bool = True, input_text: str | None = None):
    env = {key: value for key, value in os.environ.items() if not key.startswith("CHRONOS_")}
    env["CHRONOS_METADATA_URL"] = target.metadata_url
    env["CHRONOS_QDRANT_COLLECTION_PREFIX"] = target.collection_prefix
    if target.path is not None:
        env["CHRONOS_QDRANT_PATH"] = str(target.path)
    else:
        env["CHRONOS_QDRANT_URL"] = target.url
        if target.api_key is not None:
            env["CHRONOS_QDRANT_API_KEY"] = target.api_key
    result = subprocess.run(
        [sys.executable, "-m", "chronos_core.cli", "qdrant", *args],
        env=env, input=input_text, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == (0 if ok else 1), result.stderr
    assert "Traceback" not in result.stderr
    if not ok:
        assert result.stdout == ""
    return json.loads(result.stdout) if result.stdout.strip() else None


def open_api(target: QdrantConfig):
    if target.path is not None:
        return ChronosQdrantStore.local(
            target.metadata_url, path=target.path, collection_prefix=target.collection_prefix,
        )
    return ChronosQdrantStore.remote(
        target.metadata_url, url=target.url, api_key=target.api_key,
        collection_prefix=target.collection_prefix,
    )


def test_qdrant_cli_workflow(qdrant_target: QdrantConfig) -> None:
    target = qdrant_target
    info = invoke(target, "collection", "register", "documents", "--dimensions", "3")
    assert info["name"] == "documents"
    assert info["dimensions"] == 3
    assert invoke(target, "collection", "info", "documents") == info
    assert invoke(target, "collection", "list") == [info]
    first = {"vector": [1, 0, 0], "payload": {"title": "Runtime"}}
    second = {"vector": [0, 1, 0], "payload": {"title": "Billing"}}
    point = invoke(target, "point", "upsert", "documents", "runtime", json.dumps(first))
    invoke(target, "point", "upsert", "documents", "billing", json.dumps(second))
    assert invoke(target, "point", "get", "documents", "runtime") == point
    assert {item["id"] for item in invoke(target, "point", "list", "documents")} == {"runtime", "billing"}
    assert invoke(target, "point", "get", "documents", "missing") is None
    results = invoke(target, "search", "documents", "[1,0,0]", "--limit", "2")
    assert [item["id"] for item in results] == ["runtime", "billing"]
    assert results[0]["score"] == pytest.approx(1.0, abs=1e-6)
    assert results[1]["score"] == pytest.approx(0.0, abs=1e-6)
    assert len(invoke(target, "search", "documents", "[1,0,0]", "--score-threshold", "0.9")) == 1

    # Independent expected data plus a direct public-API comparison.
    store = open_api(target)
    try:
        with store.context.checkout("main") as control:
            session = store.checkout_control(control)
            assert point == asdict(session.get("documents", "runtime"))
            assert results == [asdict(item) for item in session.search("documents", [1, 0, 0], limit=2)]
    finally:
        store.close()

    invoke(target, "branch", "create", "experiment")
    assert {item["branch_id"] for item in invoke(target, "branch", "list")} == {"main", "experiment"}
    changed = {"vector": [0, 0, 1], "payload": {"title": "Experiment"}}
    invoke(target, "point", "upsert", "documents", "runtime", json.dumps(changed), "--branch", "experiment")
    assert invoke(target, "point", "delete", "documents", "billing", "--branch", "experiment") is True
    assert invoke(target, "point", "get", "documents", "billing", "--branch", "experiment") is None
    assert invoke(target, "point", "get", "documents", "billing")["payload"] == second["payload"]
    assert invoke(target, "point", "get", "documents", "runtime")["payload"] == first["payload"]
    assert invoke(target, "checkpoint", "create", "saved", "--branch", "experiment")["checkpoint_id"] == "saved"
    assert len(invoke(target, "checkpoint", "list", "--branch", "experiment")) == 1
    changes = invoke(target, "diff", "main", "experiment")["changes"]
    assert {(item["key"]["id"], item["change"]) for item in changes} == {("runtime", "modified"), ("billing", "deleted")}
    assert invoke(target, "merge", "preview", "experiment", "main")["conflicts"] == []
    assert invoke(target, "merge", "apply", "experiment", "main")["applied"] == 2
    assert invoke(target, "point", "get", "documents", "runtime")["payload"] == changed["payload"]
    assert invoke(target, "point", "get", "documents", "billing") is None
    assert invoke(target, "point", "delete", "documents", "missing") is False
    invoke(target, "branch", "delete", "experiment")


def test_qdrant_cli_conflicts_and_failed_operations(qdrant_target: QdrantConfig) -> None:
    target = qdrant_target
    invoke(target, "collection", "register", "docs", "--dimensions", "3")
    invoke(target, "point", "upsert", "docs", "one", '{"vector":[1,0,0],"payload":{"value":"base"}}')
    invoke(target, "branch", "create", "trial")
    invoke(target, "branch", "create", "trial", ok=False)
    invoke(target, "point", "upsert", "docs", "one", '{"vector":[0,1,0],"payload":{"value":"trial"}}', "--branch", "trial")
    invoke(target, "point", "upsert", "docs", "one", '{"vector":[0,0,1],"payload":{"value":"main"}}')
    assert len(invoke(target, "merge", "preview", "trial", "main")["conflicts"]) == 1
    invoke(target, "merge", "apply", "trial", "main", ok=False)
    invoke(target, "merge", "apply", "trial", "main", "--policy", "snapshot_isolation", ok=False)
    invoke(target, "point", "upsert", "docs", "bad", '{"vector":[1,0]}', ok=False)
    assert invoke(target, "point", "get", "docs", "bad") is None
    assert invoke(target, "point", "get", "docs", "one")["payload"] == {"value": "main"}
    invoke(target, "point", "get", "docs", "one", "--branch", "missing", ok=False)
    invoke(target, "collection", "info", "missing", ok=False)


def test_qdrant_cli_json_files_and_stdin(qdrant_target: QdrantConfig, tmp_path: Path) -> None:
    target = qdrant_target
    invoke(target, "collection", "register", "docs", "--dimensions", "3")
    data = tmp_path / "point.json"
    data.write_text('{"vector":[1,0,0],"payload":{"source":"file"}}', encoding="utf-8")
    result = invoke(target, "point", "upsert", "docs", "one", "--file", str(data), "--revision", "initial")
    assert result["payload"] == {"source": "file"}
    assert invoke(target, "search", "docs", "--stdin", input_text="[1,0,0]")[0]["id"] == "one"
    invoke(target, "point", "upsert", "docs", "two", "--stdin", input_text='{"vector":[0,1,0]}')
    data.write_text("[0,1,0]", encoding="utf-8")
    assert invoke(target, "search", "docs", "--file", str(data))[0]["id"] == "two"
    assert invoke(target, "search", "docs", input_text="[1,0,0]")[0]["id"] == "one"
