"""SQL input and commands for named relational workspace participants."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import sqlglot
import typer
from sqlglot import exp

from chronos_core.cli._terminal import print_result
from chronos_core.cli._workspace import connect_workspace, store_session


table_app = typer.Typer(no_args_is_help=True)


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


def _create_table_input(sql: str | None, file: Path | None, stdin: bool) -> str:
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
    return statement


@table_app.command("create")
def create_table(
    ctx: typer.Context, sql: str | None = typer.Argument(None),
    store: str = typer.Option(..., "--store"),
    file: Path | None = typer.Option(None, "--file"), stdin: bool = typer.Option(False, "--stdin"),
) -> None:
    statement = _create_table_input(sql, file, stdin)
    with connect_workspace(ctx, store) as workspace:
        database = workspace.stores[store]
        database.db.execute(statement)
        print_result(database.db.commit())


@table_app.command("register")
def register_table(ctx: typer.Context, table: str, store: str = typer.Option(..., "--store"),
                   primary_key: list[str] = typer.Option(..., "--primary-key")) -> None:
    with connect_workspace(ctx, store) as workspace:
        print_result(workspace.stores[store].register_table(table, primary_key))


def query(
    ctx: typer.Context, sql: str | None = typer.Argument(None),
    store: str = typer.Option(..., "--store"), branch: str = typer.Option("main", "--branch"),
    params: str | None = typer.Option(None, "--params"),
    file: Path | None = typer.Option(None, "--file"), stdin: bool = typer.Option(False, "--stdin"),
) -> None:
    statement, parameters = _sql_input(sql, file, stdin), _parameters(params)
    with connect_workspace(ctx, store) as workspace:
        with store_session(workspace, branch, store) as session:
            print_result(session.query(statement, parameters))


def execute(
    ctx: typer.Context, sql: str | None = typer.Argument(None),
    store: str = typer.Option(..., "--store"), branch: str = typer.Option("main", "--branch"),
    params: str | None = typer.Option(None, "--params"),
    file: Path | None = typer.Option(None, "--file"), stdin: bool = typer.Option(False, "--stdin"),
) -> None:
    statement, parameters = _sql_input(sql, file, stdin), _parameters(params)
    with connect_workspace(ctx, store) as workspace:
        with store_session(workspace, branch, store) as session:
            print_result(session.execute(statement, parameters))


def register_commands(app: typer.Typer) -> None:
    app.command()(query)
    app.command()(execute)
    app.add_typer(table_app, name="table", help="Prepare tables in a named relational store.")
