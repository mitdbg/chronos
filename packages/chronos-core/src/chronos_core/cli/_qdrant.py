"""JSON input and commands for named Qdrant workspace participants."""
from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import typer

from chronos_core.cli._terminal import print_result
from chronos_core.cli._workspace import connect_workspace, store_session

@dataclass(frozen=True)
class QdrantSelection:
    store: str | None


app = typer.Typer(no_args_is_help=True)
collection_app = typer.Typer(no_args_is_help=True)
point_app = typer.Typer(no_args_is_help=True)
app.add_typer(collection_app, name="collection", help="Register and inspect collections.")
app.add_typer(point_app, name="point", help="Write, retrieve, and delete branch-visible points.")


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


@app.callback()
def select_qdrant(ctx: typer.Context, store: str | None = typer.Option(None, "--store", help="Named Qdrant store; required for operations.")) -> None:
    ctx.obj = QdrantSelection(store)


def _qdrant_name(ctx: typer.Context) -> str:
    selection = ctx.find_object(QdrantSelection)
    if selection is None or not selection.store:
        raise typer.BadParameter("Select a Qdrant store with --store.", param_hint="--store")
    return selection.store


@collection_app.command("register")
def register_collection(ctx: typer.Context, name: str,
                        dimensions: int = typer.Option(..., "--dimensions", min=1),
                        distance: str = typer.Option("cosine", "--distance")) -> None:
    name_in_config = _qdrant_name(ctx)
    with connect_workspace(ctx, name_in_config, qdrant=True) as workspace:
        print_result(workspace.stores[name_in_config].register_collection(name, dimensions, distance=distance))


@collection_app.command("list")
def list_collections(ctx: typer.Context) -> None:
    name = _qdrant_name(ctx)
    with connect_workspace(ctx, name, qdrant=True) as workspace:
        print_result(workspace.stores[name].list_collections())


@collection_app.command("info")
def collection_info(ctx: typer.Context, name: str) -> None:
    target = _qdrant_name(ctx)
    with connect_workspace(ctx, target, qdrant=True) as workspace:
        print_result(workspace.stores[target].collection_info(name))


@point_app.command("upsert")
def upsert(
    ctx: typer.Context, collection: str, point_id: str, data: str | None = typer.Argument(None),
    file: Path | None = typer.Option(None, "--file"), stdin: bool = typer.Option(False, "--stdin"),
    branch: str = typer.Option("main", "--branch"), revision: str | None = typer.Option(None, "--revision"),
) -> None:
    vector, payload = point_input(read_json_input(data, file, stdin))
    name = _qdrant_name(ctx)
    with connect_workspace(ctx, name, qdrant=True) as workspace:
        with store_session(workspace, branch, name) as session:
            print_result(session.upsert(collection, point_id, vector, payload, revision=revision))


@point_app.command("get")
def get(ctx: typer.Context, collection: str, point_id: str, branch: str = typer.Option("main", "--branch")) -> None:
    name = _qdrant_name(ctx)
    with connect_workspace(ctx, name, qdrant=True) as workspace:
        with store_session(workspace, branch, name) as session:
            print_result(session.get(collection, point_id))


@point_app.command("list")
def list_points(ctx: typer.Context, collection: str, branch: str = typer.Option("main", "--branch")) -> None:
    name = _qdrant_name(ctx)
    with connect_workspace(ctx, name, qdrant=True) as workspace:
        with store_session(workspace, branch, name) as session:
            print_result(session.list_points(collection))


@point_app.command("delete")
def delete_point(ctx: typer.Context, collection: str, point_id: str, branch: str = typer.Option("main", "--branch")) -> None:
    name = _qdrant_name(ctx)
    with connect_workspace(ctx, name, qdrant=True) as workspace:
        with store_session(workspace, branch, name) as session:
            print_result(session.delete(collection, point_id))


@app.command()
def search(
    ctx: typer.Context, collection: str, vector: str | None = typer.Argument(None),
    file: Path | None = typer.Option(None, "--file"), stdin: bool = typer.Option(False, "--stdin"),
    branch: str = typer.Option("main", "--branch"), limit: int = typer.Option(10, "--limit"),
    score_threshold: float | None = typer.Option(None, "--score-threshold"),
) -> None:
    query_vector = dense_vector(read_json_input(vector, file, stdin))
    name = _qdrant_name(ctx)
    with connect_workspace(ctx, name, qdrant=True) as workspace:
        with store_session(workspace, branch, name) as session:
            print_result(session.search(collection, query_vector, limit=limit, score_threshold=score_threshold))
