"""Assemble the public Chronos CLI and its root connection options."""
from __future__ import annotations

import typer

from chronos_core.cli._qdrant import app as qdrant_app
from chronos_core.cli._relational import DatabaseConfig, register_commands
from chronos_core.cli._workspace import app as workspace_app

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)
register_commands(app)
app.add_typer(qdrant_app, name="qdrant", help="Branch-aware vector storage and search.")
app.add_typer(workspace_app, name="workspace", help="Coordinate configured stores, or use just one.")


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


if __name__ == "__main__":
    app()
