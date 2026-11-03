"""One-off commands over the public multi-store workspace API."""
from __future__ import annotations

import sqlite3
import sys
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
import typer

from chronos_core.cli._qdrant import QdrantConfig, dense_vector, point_input, read_json_input
from chronos_core.cli._relational import _create_table_input, _parameters, _sql_input
from chronos_core.cli._terminal import MergePolicy, print_result, report_error
from chronos_core.cli._workspace_config import WorkspaceConfigError, load_workspace_config, open_workspace

if TYPE_CHECKING:
    from chronos_core.workspace import ChronosWorkspaceContext


@dataclass(frozen=True)
class WorkspaceOptions:
    config_path: Path | None


@dataclass(frozen=True)
class QdrantSelection:
    store: str | None


app = typer.Typer(no_args_is_help=True)
branch_app = typer.Typer(no_args_is_help=True)
checkpoint_app = typer.Typer(no_args_is_help=True)
merge_app = typer.Typer(no_args_is_help=True)
table_app = typer.Typer(no_args_is_help=True)
qdrant_app = typer.Typer(no_args_is_help=True)
collection_app = typer.Typer(no_args_is_help=True)
point_app = typer.Typer(no_args_is_help=True)
app.add_typer(branch_app, name="branch", help="Manage branches across the workspace.")
app.add_typer(checkpoint_app, name="checkpoint", help="Checkpoint all participants.")
app.add_typer(merge_app, name="merge", help="Preview and apply coordinated or atomic merges.")
app.add_typer(table_app, name="table", help="Prepare tables in a named relational store.")
app.add_typer(qdrant_app, name="qdrant", help="Use a named Qdrant participant.")
qdrant_app.add_typer(collection_app, name="collection")
qdrant_app.add_typer(point_app, name="point")


@app.callback()
def main(
    ctx: typer.Context,
    config: Path | None = typer.Option(
        None, "--config", envvar="CHRONOS_WORKSPACE_CONFIG",
        help="Workspace TOML path; defaults to ./chronos.toml.",
    ),
) -> None:
    """Use configured stores together, including workspaces with just one store."""
    ctx.obj = WorkspaceOptions(config)


@contextmanager
def _connect(
    ctx: typer.Context, store: str | None = None, *, qdrant: bool = False,
) -> Generator[ChronosWorkspaceContext, None, None]:
    options = ctx.find_object(WorkspaceOptions)
    try:
        config = load_workspace_config(options.config_path if options else None)
    except WorkspaceConfigError as error:
        raise typer.BadParameter(str(error), param_hint="--config") from error
    if store is not None:
        try:
            config.store(store, qdrant=qdrant)
        except WorkspaceConfigError as error:
            raise typer.BadParameter(str(error), param_hint="--store") from error

    from chronos_core.branching import BranchingError

    errors = (BranchingError, RuntimeError, ValueError, TypeError, OSError, sqlite3.Error, psycopg.Error)
    secrets: list[str | None] = []
    urls: list[str | None] = []
    for entry in config.stores.values():
        target = entry.connection
        urls.append(target.metadata_url)
        if isinstance(target, QdrantConfig):
            secrets.append(target.api_key)
            urls.append(target.url)
        else:
            urls.append(target.database_url)
    if any(entry.kind == "qdrant" for entry in config.stores.values()):
        try:
            import grpc
            import httpx
            from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse
        except ImportError:
            report_error(RuntimeError("Qdrant support requires installing chronos-core[qdrant]."))
        errors += (grpc.RpcError, httpx.HTTPError, ResponseHandlingException, UnexpectedResponse)
    try:
        with open_workspace(config) as workspace:
            yield workspace
    except errors as error:
        report_error(error, secrets=tuple(secrets), urls=tuple(urls))


@contextmanager
def _session(workspace: ChronosWorkspaceContext, branch: str, store: str) -> Generator[Any, None, None]:
    checked_out = workspace.checkout(branch)
    errors: list[Exception] = []
    try:
        yield checked_out.stores[store]
    finally:
        # SQL sessions expose close(). Qdrant control sessions without a public
        # close method are released by their owning context when it closes.
        for session in checked_out.stores.values():
            close = getattr(session, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as error:
                    errors.append(error)
        if errors and sys.exc_info()[0] is None:
            raise errors[0]


@branch_app.command("list")
def list_branches(ctx: typer.Context) -> None:
    with _connect(ctx) as workspace:
        print_result(workspace.list_branches())


@branch_app.command("create")
def create_branch(ctx: typer.Context, name: str, from_branch: str = typer.Option("main", "--from")) -> None:
    with _connect(ctx) as workspace:
        print_result(workspace.create_branch(name, from_branch=from_branch))


@branch_app.command("create-from-checkpoint")
def create_branch_from_checkpoint(ctx: typer.Context, name: str, checkpoint: str) -> None:
    with _connect(ctx) as workspace:
        print_result(workspace.create_branch_from_checkpoint(name, checkpoint))


@branch_app.command("delete")
def delete_branch(ctx: typer.Context, name: str) -> None:
    with _connect(ctx) as workspace:
        print_result(workspace.delete_branch(name))


@checkpoint_app.command("create")
def create_checkpoint(ctx: typer.Context, name: str, branch: str = typer.Option("main", "--branch")) -> None:
    with _connect(ctx) as workspace:
        print_result(workspace.create_checkpoint(name, branch=branch))


@app.command()
def diff(ctx: typer.Context, left: str, right: str) -> None:
    with _connect(ctx) as workspace:
        print_result(workspace.diff(left, right))


@merge_app.command("preview")
def merge_preview(ctx: typer.Context, source: str, target: str,
                  policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy")) -> None:
    with _connect(ctx) as workspace:
        print_result(workspace.merge_preview(source, target, policy=policy.value))


@merge_app.command("apply")
def merge_apply(ctx: typer.Context, source: str, target: str,
                policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy")) -> None:
    with _connect(ctx) as workspace:
        print_result(workspace.merge_apply(source, target, policy=policy.value))


@merge_app.command("atomic-preview")
def merge_atomic_preview(ctx: typer.Context, source: str, target: str,
                         policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy")) -> None:
    with _connect(ctx) as workspace:
        print_result(workspace.merge_atomic_preview(source, target, policy=policy.value))


@merge_app.command("atomic-apply")
def merge_atomic(
    ctx: typer.Context, source: str, target: str,
    operation_id: str = typer.Option(..., "--operation-id"),
    preview_token: str | None = typer.Option(None, "--preview-token"),
    policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy"),
) -> None:
    with _connect(ctx) as workspace:
        print_result(workspace.merge_atomic(
            source, target, operation_id=operation_id, preview_token=preview_token, policy=policy.value,
        ))


@table_app.command("create")
def create_table(
    ctx: typer.Context, sql: str | None = typer.Argument(None),
    store: str = typer.Option(..., "--store"),
    file: Path | None = typer.Option(None, "--file"), stdin: bool = typer.Option(False, "--stdin"),
) -> None:
    statement = _create_table_input(sql, file, stdin)
    with _connect(ctx, store) as workspace:
        database = workspace.stores[store]
        database.db.execute(statement)
        print_result(database.db.commit())


@table_app.command("register")
def register_table(ctx: typer.Context, table: str, store: str = typer.Option(..., "--store"),
                   primary_key: list[str] = typer.Option(..., "--primary-key")) -> None:
    with _connect(ctx, store) as workspace:
        print_result(workspace.stores[store].register_table(table, primary_key))


@app.command()
def query(
    ctx: typer.Context, sql: str | None = typer.Argument(None),
    store: str = typer.Option(..., "--store"), branch: str = typer.Option("main", "--branch"),
    params: str | None = typer.Option(None, "--params"),
    file: Path | None = typer.Option(None, "--file"), stdin: bool = typer.Option(False, "--stdin"),
) -> None:
    statement, parameters = _sql_input(sql, file, stdin), _parameters(params)
    with _connect(ctx, store) as workspace:
        with _session(workspace, branch, store) as session:
            print_result(session.query(statement, parameters))


@app.command()
def execute(
    ctx: typer.Context, sql: str | None = typer.Argument(None),
    store: str = typer.Option(..., "--store"), branch: str = typer.Option("main", "--branch"),
    params: str | None = typer.Option(None, "--params"),
    file: Path | None = typer.Option(None, "--file"), stdin: bool = typer.Option(False, "--stdin"),
) -> None:
    statement, parameters = _sql_input(sql, file, stdin), _parameters(params)
    with _connect(ctx, store) as workspace:
        with _session(workspace, branch, store) as session:
            print_result(session.execute(statement, parameters))


@qdrant_app.callback()
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
    with _connect(ctx, name_in_config, qdrant=True) as workspace:
        print_result(workspace.stores[name_in_config].register_collection(name, dimensions, distance=distance))


@collection_app.command("list")
def list_collections(ctx: typer.Context) -> None:
    name = _qdrant_name(ctx)
    with _connect(ctx, name, qdrant=True) as workspace:
        print_result(workspace.stores[name].list_collections())


@collection_app.command("info")
def collection_info(ctx: typer.Context, name: str) -> None:
    target = _qdrant_name(ctx)
    with _connect(ctx, target, qdrant=True) as workspace:
        print_result(workspace.stores[target].collection_info(name))


@point_app.command("upsert")
def upsert(
    ctx: typer.Context, collection: str, point_id: str, data: str | None = typer.Argument(None),
    file: Path | None = typer.Option(None, "--file"), stdin: bool = typer.Option(False, "--stdin"),
    branch: str = typer.Option("main", "--branch"), revision: str | None = typer.Option(None, "--revision"),
) -> None:
    vector, payload = point_input(read_json_input(data, file, stdin))
    name = _qdrant_name(ctx)
    with _connect(ctx, name, qdrant=True) as workspace:
        with _session(workspace, branch, name) as session:
            print_result(session.upsert(collection, point_id, vector, payload, revision=revision))


@point_app.command("get")
def get(ctx: typer.Context, collection: str, point_id: str, branch: str = typer.Option("main", "--branch")) -> None:
    name = _qdrant_name(ctx)
    with _connect(ctx, name, qdrant=True) as workspace:
        with _session(workspace, branch, name) as session:
            print_result(session.get(collection, point_id))


@point_app.command("list")
def list_points(ctx: typer.Context, collection: str, branch: str = typer.Option("main", "--branch")) -> None:
    name = _qdrant_name(ctx)
    with _connect(ctx, name, qdrant=True) as workspace:
        with _session(workspace, branch, name) as session:
            print_result(session.list_points(collection))


@point_app.command("delete")
def delete_point(ctx: typer.Context, collection: str, point_id: str, branch: str = typer.Option("main", "--branch")) -> None:
    name = _qdrant_name(ctx)
    with _connect(ctx, name, qdrant=True) as workspace:
        with _session(workspace, branch, name) as session:
            print_result(session.delete(collection, point_id))


@qdrant_app.command()
def search(
    ctx: typer.Context, collection: str, vector: str | None = typer.Argument(None),
    file: Path | None = typer.Option(None, "--file"), stdin: bool = typer.Option(False, "--stdin"),
    branch: str = typer.Option("main", "--branch"), limit: int = typer.Option(10, "--limit"),
    score_threshold: float | None = typer.Option(None, "--score-threshold"),
) -> None:
    query_vector = dense_vector(read_json_input(vector, file, stdin))
    name = _qdrant_name(ctx)
    with _connect(ctx, name, qdrant=True) as workspace:
        with _session(workspace, branch, name) as session:
            print_result(session.search(collection, query_vector, limit=limit, score_threshold=score_threshold))
