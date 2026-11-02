"""Command parsing, API forwarding, errors, and resource ownership for Qdrant."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from chronos_core.cli import _qdrant_support as support
from chronos_core.cli.main import app

runner = CliRunner()


@pytest.fixture
def factory(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    pytest.importorskip("qdrant_client")
    for name in ("CHRONOS_METADATA_URL", "CHRONOS_QDRANT_PATH", "CHRONOS_QDRANT_URL",
                 "CHRONOS_QDRANT_API_KEY", "CHRONOS_QDRANT_COLLECTION_PREFIX"):
        monkeypatch.delenv(name, raising=False)
    factory = MagicMock()
    factory.remote.return_value = factory.local.return_value
    monkeypatch.setattr(support, "_store_class", lambda: factory)
    return factory


def invoke(*args: str, **kwargs):
    return runner.invoke(app, ["qdrant", "--path", "vectors", "--metadata-url", "sqlite:///metadata.sqlite", *args], **kwargs)


def test_help_does_not_connect(factory: MagicMock) -> None:
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
def test_store_api_forwarding(factory: MagicMock, args, method, positional, keywords) -> None:
    store = factory.local.return_value
    getattr(store, method).return_value = {"result": "from API"}
    result = invoke(*args)
    assert result.exit_code == 0, result.output
    getattr(store, method).assert_called_once_with(*positional, **keywords)
    assert json.loads(result.stdout) == {"result": "from API"}
    store.close.assert_called_once()


@pytest.mark.parametrize(("args", "method", "keywords"), [
    (["branch", "list"], "list_branches", {}),
    (["checkpoint", "list", "--branch", "trial"], "list_checkpoints", {"branch": "trial"}),
])
def test_metadata_context_calls(factory: MagicMock, args, method, keywords) -> None:
    store = factory.local.return_value
    getattr(store.context, method).return_value = []
    result = invoke(*args)
    assert result.exit_code == 0, result.output
    getattr(store.context, method).assert_called_once_with(**keywords)
    assert json.loads(result.stdout) == []
    store.close.assert_called_once()


@pytest.mark.parametrize(("command", "method", "positional"), [
    (["point", "get", "docs", "one"], "get", ("docs", "one")),
    (["point", "list", "docs"], "list_points", ("docs",)),
    (["point", "delete", "docs", "one"], "delete", ("docs", "one")),
])
def test_point_session_calls(factory: MagicMock, command, method, positional) -> None:
    store = factory.local.return_value
    session = store.checkout_control.return_value
    getattr(session, method).return_value = False if method == "delete" else None
    result = invoke(*command, "--branch", "trial")
    assert result.exit_code == 0, result.output
    store.context.checkout.assert_called_once_with("trial")
    store.checkout_control.assert_called_once_with(store.context.checkout.return_value.__enter__.return_value)
    getattr(session, method).assert_called_once_with(*positional)
    assert result.stdout == ("false\n" if method == "delete" else "")
    store.context.checkout.return_value.__exit__.assert_called_once()
    store.close.assert_called_once()


@pytest.mark.parametrize("source", ["text", "file", "stdin", "pipe"])
@pytest.mark.parametrize("command", ["upsert", "search"])
def test_json_sources(factory: MagicMock, tmp_path: Path, source: str, command: str) -> None:
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
    result = invoke(*args, input=input_text)
    assert result.exit_code == 0, result.output
    if command == "upsert":
        session.upsert.assert_called_once_with("docs", "one", [1, 0, 0], {"title": "hello"}, revision="r1")
    else:
        session.search.assert_called_once_with("docs", [1, 0, 0], limit=2, score_threshold=0.5)
    assert json.loads(result.stdout) == []
    store.close.assert_called_once()


def test_connection_flags_and_environment(factory: MagicMock) -> None:
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
def test_invalid_configuration_does_not_connect(factory: MagicMock, args) -> None:
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
def test_invalid_input_does_not_connect(factory: MagicMock, args) -> None:
    result = invoke(*args)
    assert result.exit_code == 2, result.output
    factory.local.assert_not_called()


def test_failure_closes_session_before_store_and_redacts_secrets(factory: MagicMock) -> None:
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


def test_factory_failure_does_not_close_unassigned_store(factory: MagicMock) -> None:
    factory.local.side_effect = ValueError("Cannot open storage")
    result = invoke("collection", "list")
    assert result.exit_code == 1
    factory.local.return_value.close.assert_not_called()


def test_optional_dependency_is_not_needed_for_help(tmp_path: Path) -> None:
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
        result = subprocess.run([sys.executable, "-c", code, *args], capture_output=True, text=True, env=env)
        assert result.returncode == 0, result.stderr
    result = subprocess.run([sys.executable, "-c", code, "qdrant", "--metadata-url", f"sqlite:///{tmp_path / 'metadata.sqlite'}",
                             "--path", str(tmp_path / "vectors"), "collection", "list"], capture_output=True, text=True, env=env)
    assert result.returncode == 1
    assert "chronos-core[qdrant]" in result.stderr
    assert list(tmp_path.iterdir()) == []
    result = subprocess.run([sys.executable, "-c", code, "--database-url", f"sqlite:///{tmp_path / 'sql.sqlite'}",
                             "branch", "list"], capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr
    assert [branch["branch_id"] for branch in json.loads(result.stdout)] == ["main"]
