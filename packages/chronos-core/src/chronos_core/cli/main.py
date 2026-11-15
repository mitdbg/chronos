"""Assemble the workspace CLI and its root configuration option."""
from __future__ import annotations

from pathlib import Path

import typer

from chronos_core.cli._filesystem import app as filesystem_app
from chronos_core.cli._qdrant import app as qdrant_app
from chronos_core.cli._relational import register_commands as register_relational_commands
from chronos_core.cli._workspace import WorkspaceOptions, held_workspace, register_commands as register_workspace_commands

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)
app.add_typer(filesystem_app, name="fs", help="Use the configured workspace filesystem.")
register_workspace_commands(app)
register_relational_commands(app)
app.add_typer(qdrant_app, name="qdrant", help="Use a named Qdrant participant.")


def _from_command_line(ctx: typer.Context, name: str) -> bool:
    # Compare by name: typer >= 0.29 bundles its own click instead of depending on it.
    source = ctx.get_parameter_source(name)
    return source is not None and source.name == "COMMANDLINE"


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    config: Path | None = typer.Option(
        None, "--config", envvar="CHRONOS_WORKSPACE_CONFIG",
        help="Workspace TOML path; defaults to ./chronos.toml.",
    ),
    persistent: bool = typer.Option(
        False, "--persistent",
        help="Open the workspace once, run one command per stdin line, and print one JSON response per line.",
    ),
) -> None:
    """Use configured Chronos stores together, including workspaces with just one store."""
    if held_workspace() is not None:
        # A command line inside persistent mode: the workspace is already open.
        if persistent or _from_command_line(ctx, "config"):
            raise typer.BadParameter("Persistent mode fixes the workspace at startup.",
                                     param_hint="--persistent" if persistent else "--config")
        return
    ctx.obj = WorkspaceOptions(config)
    if persistent:
        if ctx.invoked_subcommand is not None:
            raise typer.BadParameter("Commands are read from stdin in persistent mode.", param_hint="--persistent")
        from chronos_core.cli._persistent import run_persistent

        run_persistent(ctx, typer.main.get_command(app))
        raise typer.Exit(0)
    if ctx.invoked_subcommand is None:
        ctx.fail("Missing command.")


if __name__ == "__main__":
    app()
