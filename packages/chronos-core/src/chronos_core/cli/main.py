"""Assemble the workspace CLI and its root configuration option."""
from __future__ import annotations

from pathlib import Path

import typer

from chronos_core.cli._filesystem import app as filesystem_app
from chronos_core.cli._qdrant import app as qdrant_app
from chronos_core.cli._relational import register_commands as register_relational_commands
from chronos_core.cli._workspace import WorkspaceOptions, register_commands as register_workspace_commands

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)
app.add_typer(filesystem_app, name="fs", help="Use the configured workspace filesystem.")
register_workspace_commands(app)
register_relational_commands(app)
app.add_typer(qdrant_app, name="qdrant", help="Use a named Qdrant participant.")


@app.callback()
def main(
    ctx: typer.Context,
    config: Path | None = typer.Option(
        None, "--config", envvar="CHRONOS_WORKSPACE_CONFIG",
        help="Workspace TOML path; defaults to ./chronos.toml.",
    ),
) -> None:
    """Use configured Chronos stores together, including workspaces with just one store."""
    ctx.obj = WorkspaceOptions(config)


if __name__ == "__main__":
    app()
