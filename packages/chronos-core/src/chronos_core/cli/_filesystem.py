"""Terminal access to the workspace's public filesystem session API."""
from __future__ import annotations

import json
import math
import subprocess

import typer

from chronos_core.cli._terminal import print_result
from chronos_core.cli._workspace import connect_workspace, workspace_session

app = typer.Typer(no_args_is_help=True)


@app.command("run", context_settings={"allow_interspersed_args": False})
def run(
    ctx: typer.Context,
    argv: list[str] = typer.Argument(..., help="Command and arguments; use -- before the command."),
    branch: str = typer.Option("main", "--branch"),
    cwd: str = typer.Option(".", "--cwd", help="Working directory relative to the branch mount."),
    env: str | None = typer.Option(None, "--env", help="JSON object replacing the child environment; omitted inherits it."),
    timeout: float | None = typer.Option(None, "--timeout", min=0.001),
    as_json: bool = typer.Option(False, "--json", help="Print args, returncode, stdout, and stderr as JSON."),
) -> None:
    """Run a command, print captured output after completion, then release mounts."""
    if timeout is not None and not math.isfinite(timeout):
        raise typer.BadParameter("Expected a finite timeout.", param_hint="--timeout")
    environment = None
    if env is not None:
        try:
            environment = json.loads(env)
        except ValueError as error:
            raise typer.BadParameter("Expected a JSON object of string values.", param_hint="--env") from error
        if not isinstance(environment, dict) or not all(isinstance(value, str) for value in environment.values()):
            raise typer.BadParameter("Expected a JSON object of string values.", param_hint="--env")
    with connect_workspace(ctx) as workspace:
        if workspace.filesystem is None:
            raise typer.BadParameter("Configure [filesystem] to run filesystem commands.")
        try:
            with workspace_session(workspace, branch) as checked_out:
                result = checked_out.fs.run(argv, cwd=cwd, env=environment, timeout=timeout)
        except subprocess.TimeoutExpired:
            # Avoid echoing argv: it can contain credentials supplied to the child.
            typer.echo("Error: filesystem command timed out.", err=True)
            raise typer.Exit(1) from None
        if as_json:
            print_result({"args": result.args, "returncode": result.returncode,
                          "stdout": result.stdout, "stderr": result.stderr})
        else:
            typer.echo(result.stdout, nl=False, color=True)
            typer.echo(result.stderr, err=True, nl=False, color=True)
        # subprocess uses negative return codes for signals; shells use 128+signal.
        status = result.returncode if result.returncode >= 0 else 128 - result.returncode
    raise typer.Exit(status)
