"""One-off terminal commands wrapping public Chronos APIs."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import typer
import sqlglot
from sqlglot import exp

from chronos_core.cli._support import DatabaseConfig, MergePolicy, connect, database_config, print_result
from chronos_core.cli.qdrant import app as qdrant_app

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)
branch_app = typer.Typer(no_args_is_help=True)
table_app = typer.Typer(no_args_is_help=True)
checkpoint_app = typer.Typer(no_args_is_help=True)
merge_app = typer.Typer(no_args_is_help=True)
app.add_typer(branch_app, name="branch", help="Manage branches.")
app.add_typer(table_app, name="table", help="Create ordinary tables and register them with Chronos.")
app.add_typer(checkpoint_app, name="checkpoint", help="Create and list checkpoints.")
app.add_typer(merge_app, name="merge", help="Preview and apply merges.")
app.add_typer(qdrant_app, name="qdrant", help="Branch-aware vector storage and search.")


@app.callback()
def main(
    ctx: typer.Context,
    database_url: str | None = typer.Option(
        None, "--database-url", envvar="CHRONOS_DATABASE_URL", help="Database connection URL."
    ),
    metadata_url: str | None = typer.Option(
        None, "--metadata-url", envvar="CHRONOS_METADATA_URL",
        help="Separate SQLite or PostgreSQL metadata URL; required for DuckDB.",
    ),
) -> None:
    ctx.obj = DatabaseConfig(database_url, metadata_url) if database_url else None


@branch_app.command("list")
def list_branches(ctx: typer.Context) -> None:
    with connect(database_config(ctx)) as database:
        print_result(database.list_branches())


@branch_app.command("create")
def create_branch(
    ctx: typer.Context, name: str,
    from_branch: str = typer.Option("main", "--from"),
) -> None:
    with connect(database_config(ctx)) as database:
        print_result(database.create_branch(name, from_branch=from_branch))


@branch_app.command("delete")
def delete_branch(ctx: typer.Context, name: str) -> None:
    with connect(database_config(ctx)) as database:
        print_result(database.delete_branch(name))


@table_app.command("register")
def register_table(
    ctx: typer.Context, table: str,
    primary_key: list[str] = typer.Option(..., "--primary-key"),
) -> None:
    with connect(database_config(ctx)) as database:
        print_result(database.register_table(table, primary_key))


def _sql_input(sql: str | None, file: Path | None, stdin: bool) -> str:
    if sum((sql is not None, file is not None, stdin)) > 1:
        raise typer.BadParameter("Choose SQL text, --file, or --stdin, not multiple sources.")
    if file is not None:
        try:
            sql = file.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise typer.BadParameter(f"Cannot read SQL file: {error}", param_hint="--file") from error
    elif stdin or (sql is None and not sys.stdin.isatty()):
        try:
            sql = sys.stdin.read()
        except (OSError, UnicodeError) as error:
            typer.echo(f"Error reading stdin: {error}", err=True)
            raise typer.Exit(1) from error
    if sql is None or not sql.strip():
        raise typer.BadParameter("Provide SQL text, --file, or SQL on stdin.")
    return sql


def _parameters(params: str | None) -> dict[str, Any] | None:
    if params is None:
        return None
    try:
        value = json.loads(params)
    except ValueError as error:
        raise typer.BadParameter("Expected a JSON object.", param_hint="--params") from error
    if not isinstance(value, dict):
        raise typer.BadParameter("Expected a JSON object.", param_hint="--params")
    return value


@table_app.command("create")
def create_table(
    ctx: typer.Context,
    sql: str | None = typer.Argument(None),
    file: Path | None = typer.Option(None, "--file"),
    stdin: bool = typer.Option(False, "--stdin"),
) -> None:
    """Create an ordinary table for subsequent table register; not a branch-local write."""
    statement = _sql_input(sql, file, stdin)
    try:
        statements = sqlglot.parse(statement)
    except sqlglot.errors.ParseError as error:
        raise typer.BadParameter("Expected one CREATE TABLE statement.") from error
    if (
        len(statements) != 1
        or not isinstance(statements[0], exp.Create)
        or statements[0].kind != "TABLE"
    ):
        raise typer.BadParameter("Expected one CREATE TABLE statement.")
    with connect(database_config(ctx)) as database:
        database.db.execute(statement)
        print_result(database.db.commit())


@app.command()
def query(
    ctx: typer.Context,
    sql: str | None = typer.Argument(None),
    branch: str = typer.Option("main", "--branch"),
    params: str | None = typer.Option(None, "--params"),
    file: Path | None = typer.Option(None, "--file"),
    stdin: bool = typer.Option(False, "--stdin"),
) -> None:
    """Call the branch session's query API with SQL and optional named parameters."""
    statement, parameters = _sql_input(sql, file, stdin), _parameters(params)
    with connect(database_config(ctx)) as database:
        with database.checkout(branch) as session:
            print_result(session.query(statement, parameters))


@app.command()
def execute(
    ctx: typer.Context,
    sql: str | None = typer.Argument(None),
    branch: str = typer.Option("main", "--branch"),
    params: str | None = typer.Option(None, "--params"),
    file: Path | None = typer.Option(None, "--file"),
    stdin: bool = typer.Option(False, "--stdin"),
) -> None:
    """Call the branch session's execute API; SQL is not split into a script."""
    statement, parameters = _sql_input(sql, file, stdin), _parameters(params)
    with connect(database_config(ctx)) as database:
        with database.checkout(branch) as session:
            print_result(session.execute(statement, parameters))


@checkpoint_app.command("create")
def create_checkpoint(
    ctx: typer.Context, name: str,
    branch: str = typer.Option("main", "--branch"),
) -> None:
    with connect(database_config(ctx)) as database:
        print_result(database.create_checkpoint(name, branch=branch))


@checkpoint_app.command("list")
def list_checkpoints(
    ctx: typer.Context, branch: str | None = typer.Option(None, "--branch"),
) -> None:
    with connect(database_config(ctx)) as database:
        print_result(database.list_checkpoints(branch=branch))


@app.command()
def diff(
    ctx: typer.Context, left: str, right: str,
    table: str | None = typer.Option(None, "--table"),
) -> None:
    with connect(database_config(ctx)) as database:
        print_result(database.diff(left, right) if table is None else database.diff_rows(left, right, table))


@merge_app.command("preview")
def merge_preview(
    ctx: typer.Context, source: str, target: str,
    policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy"),
) -> None:
    with connect(database_config(ctx)) as database:
        print_result(database.merge_preview(source, target, policy=policy.value))


@merge_app.command("apply")
def merge_apply(
    ctx: typer.Context, source: str, target: str,
    policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy"),
) -> None:
    with connect(database_config(ctx)) as database:
        print_result(database.merge_apply(source, target, policy=policy.value))


if __name__ == "__main__":
    app()
