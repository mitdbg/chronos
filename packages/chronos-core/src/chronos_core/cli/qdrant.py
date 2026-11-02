"""Terminal commands wrapping Chronos's public Qdrant store API."""
from __future__ import annotations

from pathlib import Path

import typer

from chronos_core.cli._qdrant_support import (
    QdrantConfig,
    dense_vector,
    open_qdrant,
    point_input,
    qdrant_config,
    qdrant_session,
    read_json_input,
)
from chronos_core.cli._support import MergePolicy, print_result

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
