"""Qdrant commands, JSON input, and ownership of public API resources."""
from __future__ import annotations

import json
import math
import sqlite3
import sys
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
import typer

from chronos_core.cli._terminal import MergePolicy, print_result, report_error

if TYPE_CHECKING:
    from chronos_core.workspace.qdrant import ChronosQdrantStore, QdrantBranchSession


@dataclass(frozen=True)
class QdrantConfig:
    metadata_url: str
    path: Path | None = None
    url: str | None = None
    api_key: str | None = None
    collection_prefix: str = "chronos_"
    timeout: float | None = None


def qdrant_config(ctx: typer.Context) -> QdrantConfig:
    config = ctx.find_object(QdrantConfig)
    if config is None or not config.metadata_url:
        raise typer.BadParameter("Provide --metadata-url or CHRONOS_METADATA_URL.", param_hint="--metadata-url")
    if (config.path is None) == (config.url is None):
        raise typer.BadParameter("Choose exactly one of --path or --url.")
    if config.metadata_url in {":memory:", "sqlite:///:memory:"} or str(config.path) == ":memory:":
        raise typer.BadParameter("One-off commands require persistent storage, not :memory:.")
    if config.path is not None and (config.api_key is not None or config.timeout is not None):
        raise typer.BadParameter("--api-key and --timeout require --url.")
    return config


def _store_class() -> type[ChronosQdrantStore]:
    # Optional dependencies must not be imported when showing help or using SQL.
    from chronos_core.workspace.qdrant import ChronosQdrantStore
    return ChronosQdrantStore


@contextmanager
def open_qdrant(config: QdrantConfig) -> Generator[ChronosQdrantStore, None, None]:
    try:
        import grpc
        import httpx
        from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse
        from chronos_core.branching import BranchingError
    except ImportError:
        report_error(RuntimeError("Qdrant support requires installing chronos-core[qdrant]."))

    try:
        factory = _store_class()
        if config.path is not None:
            store = factory.local(
                config.metadata_url, path=config.path, collection_prefix=config.collection_prefix,
            )
        elif config.url is not None:
            store = factory.remote(
                config.metadata_url, url=config.url, api_key=config.api_key,
                collection_prefix=config.collection_prefix, timeout=config.timeout,
            )
        else:
            raise ValueError("Choose exactly one of --path or --url.")
        try:
            yield store
        finally:
            store.close()
    except (
        BranchingError, RuntimeError, ValueError, TypeError, OSError, sqlite3.Error,
        psycopg.Error, httpx.HTTPError, grpc.RpcError, ResponseHandlingException, UnexpectedResponse,
    ) as error:
        report_error(error, secrets=(config.api_key,), urls=(config.metadata_url, config.url))


@contextmanager
def qdrant_session(store: ChronosQdrantStore, branch: str) -> Generator[QdrantBranchSession, None, None]:
    # Qdrant sessions wrap a relational control session but have no close method.
    # Both of these are public APIs; keep control-session ownership explicit.
    with store.context.checkout(branch) as control:
        yield store.checkout_control(control)


def read_json_input(text: str | None, file: Path | None, stdin: bool) -> Any:
    if sum((text is not None, file is not None, stdin)) > 1:
        raise typer.BadParameter("Choose JSON text, --file, or --stdin, not multiple sources.")
    try:
        if file is not None:
            text = file.read_text(encoding="utf-8")
        elif stdin or (text is None and not sys.stdin.isatty()):
            text = sys.stdin.read()
    except (OSError, UnicodeError) as error:
        raise typer.BadParameter(f"Cannot read JSON input: {error}") from error
    if text is None or not text.strip():
        raise typer.BadParameter("Provide JSON text, --file, or JSON on stdin.")
    try:
        return json.loads(text, parse_constant=_reject_constant)
    except ValueError as error:
        raise typer.BadParameter(f"Invalid JSON: {error}") from error


def _reject_constant(value: str) -> None:
    raise ValueError(f"{value} is not a finite JSON number")


def dense_vector(value: Any) -> list[float]:
    try:
        valid = isinstance(value, list) and all(
            isinstance(item, (int, float)) and not isinstance(item, bool) and math.isfinite(item)
            for item in value
        )
    except OverflowError:
        valid = False
    if not valid:
        raise typer.BadParameter("Expected a JSON array of finite numbers for the vector.")
    return value


def point_input(value: Any) -> tuple[list[float], dict[str, Any] | None]:
    if not isinstance(value, dict) or "vector" not in value or set(value) - {"vector", "payload"}:
        raise typer.BadParameter("Expected an object with vector and optional payload fields.")
    payload = value.get("payload")
    if payload is not None and not isinstance(payload, dict):
        raise typer.BadParameter("The payload must be a JSON object or null.")
    return dense_vector(value["vector"]), payload


app = typer.Typer(no_args_is_help=True)
collection_app = typer.Typer(no_args_is_help=True)
point_app = typer.Typer(no_args_is_help=True)
branch_app = typer.Typer(no_args_is_help=True)
checkpoint_app = typer.Typer(no_args_is_help=True)
merge_app = typer.Typer(no_args_is_help=True)
app.add_typer(collection_app, name="collection", help="Register and inspect collections.")
app.add_typer(point_app, name="point", help="Write, retrieve, and delete branch-visible points.")
app.add_typer(branch_app, name="branch", help="Manage Qdrant branches.")
app.add_typer(checkpoint_app, name="checkpoint", help="Create and list checkpoints.")
app.add_typer(merge_app, name="merge", help="Preview and apply Qdrant merges.")


@app.callback()
def main(
    ctx: typer.Context,
    metadata_url: str | None = typer.Option(None, "--metadata-url", envvar="CHRONOS_METADATA_URL"),
    path: Path | None = typer.Option(None, "--path", envvar="CHRONOS_QDRANT_PATH", file_okay=False),
    url: str | None = typer.Option(None, "--url", envvar="CHRONOS_QDRANT_URL"),
    api_key: str | None = typer.Option(None, "--api-key", envvar="CHRONOS_QDRANT_API_KEY", show_default=False),
    collection_prefix: str = typer.Option("chronos_", "--collection-prefix", envvar="CHRONOS_QDRANT_COLLECTION_PREFIX"),
    timeout: float | None = typer.Option(None, "--timeout", min=0.001, help="Remote transport timeout in seconds."),
) -> None:
    """Use persistent local Qdrant (--path) or a server (--url) with Chronos metadata."""
    ctx.obj = QdrantConfig(metadata_url or "", path, url, api_key, collection_prefix, timeout)


@collection_app.command("register")
def register_collection(
    ctx: typer.Context, name: str,
    dimensions: int = typer.Option(..., "--dimensions", min=1),
    distance: str = typer.Option("cosine", "--distance", help="cosine, dot, euclid, or manhattan."),
) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.register_collection(name, dimensions, distance=distance))


@collection_app.command("list")
def list_collections(ctx: typer.Context) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.list_collections())


@collection_app.command("info")
def collection_info(ctx: typer.Context, name: str) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.collection_info(name))


@point_app.command("upsert")
def upsert(
    ctx: typer.Context, collection: str, point_id: str,
    data: str | None = typer.Argument(None, help='JSON: {"vector": [...], "payload": {...}}'),
    file: Path | None = typer.Option(None, "--file"),
    stdin: bool = typer.Option(False, "--stdin"),
    branch: str = typer.Option("main", "--branch"),
    revision: str | None = typer.Option(None, "--revision"),
) -> None:
    vector, payload = point_input(read_json_input(data, file, stdin))
    with open_qdrant(qdrant_config(ctx)) as store:
        with qdrant_session(store, branch) as session:
            print_result(session.upsert(collection, point_id, vector, payload, revision=revision))


@point_app.command("get")
def get(ctx: typer.Context, collection: str, point_id: str, branch: str = typer.Option("main", "--branch")) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        with qdrant_session(store, branch) as session:
            print_result(session.get(collection, point_id))


@point_app.command("list")
def list_points(ctx: typer.Context, collection: str, branch: str = typer.Option("main", "--branch")) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        with qdrant_session(store, branch) as session:
            print_result(session.list_points(collection))


@point_app.command("delete")
def delete(ctx: typer.Context, collection: str, point_id: str, branch: str = typer.Option("main", "--branch")) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        with qdrant_session(store, branch) as session:
            print_result(session.delete(collection, point_id))


@app.command()
def search(
    ctx: typer.Context, collection: str,
    vector: str | None = typer.Argument(None, help="Query vector as a JSON array."),
    file: Path | None = typer.Option(None, "--file"),
    stdin: bool = typer.Option(False, "--stdin"),
    branch: str = typer.Option("main", "--branch"),
    limit: int = typer.Option(10, "--limit"),
    score_threshold: float | None = typer.Option(None, "--score-threshold"),
) -> None:
    query_vector = dense_vector(read_json_input(vector, file, stdin))
    with open_qdrant(qdrant_config(ctx)) as store:
        with qdrant_session(store, branch) as session:
            print_result(session.search(collection, query_vector, limit=limit, score_threshold=score_threshold))


@branch_app.command("list")
def list_branches(ctx: typer.Context) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.context.list_branches())


@branch_app.command("create")
def create_branch(ctx: typer.Context, name: str, from_branch: str = typer.Option("main", "--from")) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.create_branch(name, from_branch=from_branch))


@branch_app.command("delete")
def delete_branch(ctx: typer.Context, name: str) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.delete_branch(name))


@checkpoint_app.command("create")
def create_checkpoint(ctx: typer.Context, name: str, branch: str = typer.Option("main", "--branch")) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.create_checkpoint(name, branch=branch))


@checkpoint_app.command("list")
def list_checkpoints(ctx: typer.Context, branch: str | None = typer.Option(None, "--branch")) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.context.list_checkpoints(branch=branch))


@app.command()
def diff(ctx: typer.Context, left: str, right: str) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.diff(left, right))


@merge_app.command("preview")
def merge_preview(ctx: typer.Context, source: str, target: str,
                  policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy")) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.merge_preview(source, target, policy=policy.value))


@merge_app.command("apply")
def merge_apply(ctx: typer.Context, source: str, target: str,
                policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy")) -> None:
    with open_qdrant(qdrant_config(ctx)) as store:
        print_result(store.merge_apply(source, target, policy=policy.value))
