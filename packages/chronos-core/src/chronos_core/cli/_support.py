"""Shared configuration, connection lifecycle, and JSON output."""
from __future__ import annotations

import base64
import json
import re
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, NoReturn
from urllib.parse import urlsplit

import psycopg
import typer

if TYPE_CHECKING:
    from chronos_core.branching import ChronosBranchContext


@dataclass(frozen=True)
class DatabaseConfig:
    database_url: str
    metadata_url: str | None = None


class MergePolicy(str, Enum):
    abort_on_conflict = "abort_on_conflict"
    snapshot_isolation = "snapshot_isolation"


def database_config(ctx: typer.Context) -> DatabaseConfig:
    database = ctx.find_object(DatabaseConfig)
    if database is None or not database.database_url:
        raise typer.BadParameter(
            "Provide --database-url or set CHRONOS_DATABASE_URL.",
            param_hint="--database-url",
        )
    if urlsplit(database.database_url).scheme == "duckdb" and not database.metadata_url:
        raise typer.BadParameter(
            "DuckDB requires --metadata-url or CHRONOS_METADATA_URL for SQLite or PostgreSQL metadata.",
            param_hint="--metadata-url",
        )
    return database


def _json_default(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return {"encoding": "base64", "data": base64.b64encode(value).decode("ascii")}
    raise TypeError(f"Unsupported JSON value type: {type(value).__name__}")


def print_result(result: Any) -> None:
    if result is not None:
        typer.echo(json.dumps(result, default=_json_default, ensure_ascii=False, indent=2))


def report_error(
    error: Exception, *, secrets: tuple[str | None, ...] = (), urls: tuple[str | None, ...] = (),
) -> NoReturn:
    """Report an operational failure without exposing configured credentials."""
    message = str(error)
    for url in urls:
        if url:
            message = message.replace(url, "<database>")
    for secret in secrets:
        if secret:
            message = message.replace(secret, "<redacted>")
    message = re.sub(r"([a-zA-Z][\w+.-]*://)[^\s/@]+@", r"\1<credentials>@", message)
    message = re.sub(r"(?i)(password\s*=\s*)(?:'[^']*'|\"[^\"]*\"|[^\s]+)", r"\1<redacted>", message)
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(1) from error


@contextmanager
def connect(database: DatabaseConfig) -> Generator[ChronosBranchContext, None, None]:
    from chronos_core.branching import BranchingError, ChronosBranchContext

    try:
        if database.metadata_url is None:
            context = ChronosBranchContext.connect(database.database_url)
        else:
            context = ChronosBranchContext.connect_split(database.database_url, database.metadata_url)
        try:
            yield context
        finally:
            context.close()
    except (BranchingError, RuntimeError, sqlite3.Error, psycopg.Error, OSError, ValueError, TypeError) as error:
        report_error(error, urls=(database.database_url, database.metadata_url))
