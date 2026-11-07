"""Terminal access to the workspace's public filesystem session API."""
from __future__ import annotations

import json
import math
import subprocess
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import typer

from chronos_core.cli._terminal import print_result
from chronos_core.cli._workspace import connect_workspace, workspace_session

if TYPE_CHECKING:
    from chronos_core.workspace import ChronosFSBranchSession, ChronosFSStore, ChronosWorkspaceContext

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
    from chronos_core.workspace import ChronosFSStore

    with connect_workspace(ctx) as workspace:
        if isinstance(workspace.filesystem, ChronosFSStore):
            raise typer.BadParameter("fs run requires the OverlayFS backend.")
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


@contextmanager
def _native_store(ctx: typer.Context) -> Generator[tuple[ChronosWorkspaceContext, ChronosFSStore], None, None]:
    from chronos_core.workspace import ChronosFSStore

    with connect_workspace(ctx) as workspace:
        if not isinstance(workspace.filesystem, ChronosFSStore):
            raise typer.BadParameter("This command requires [filesystem] with kind = 'chronosfs'.")
        yield workspace, workspace.filesystem


@contextmanager
def _native_session(ctx: typer.Context, branch: str) -> Generator[ChronosFSBranchSession, None, None]:
    with _native_store(ctx) as (workspace, _):
        with workspace_session(workspace, branch) as session:
            yield session.fs


@app.command("mount")
def mount(
    ctx: typer.Context,
    mountpoint: Path = typer.Argument(..., exists=True, file_okay=False, resolve_path=True),
    branch: str = typer.Option("main", "--branch"),
    options: list[str] | None = typer.Option(None, "--option", "-o", help="FUSE option. May be repeated."),
) -> None:
    """Mount a native ChronosFS branch in the foreground until unmounted."""
    from chronos_core.workspace import mount_chronosfs

    with _native_store(ctx) as (_, filesystem):
        mount_chronosfs(filesystem, mountpoint, branch_id=branch, options=set(options or ()))


@app.command("ls")
def list_directory(
    ctx: typer.Context,
    path: str = typer.Argument("/"),
    branch: str = typer.Option("main", "--branch"),
) -> None:
    """List entries in a native ChronosFS directory."""
    with _native_session(ctx, branch) as filesystem:
        print_result(filesystem.listdir(path))


@app.command("read")
def read_file(
    ctx: typer.Context,
    path: str,
    branch: str = typer.Option("main", "--branch"),
) -> None:
    """Write native ChronosFS file bytes to stdout."""
    with _native_session(ctx, branch) as filesystem:
        data = filesystem.read_file(path)
    typer.get_binary_stream("stdout").write(data)


@app.command("write")
def write_file(
    ctx: typer.Context,
    path: str,
    branch: str = typer.Option("main", "--branch"),
    file: Path | None = typer.Option(None, "--file", exists=True, dir_okay=False, readable=True),
    stdin: bool = typer.Option(False, "--stdin"),
    parents: bool = typer.Option(False, "--parents", help="Create missing parent directories."),
) -> None:
    """Write bytes to a native ChronosFS file."""
    if (file is not None) == stdin:
        raise typer.BadParameter("Choose exactly one of --file or --stdin.")
    try:
        data = file.read_bytes() if file is not None else typer.get_binary_stream("stdin").read()
    except OSError as error:
        raise typer.BadParameter("Could not read the input bytes.") from error
    with _native_session(ctx, branch) as filesystem:
        print_result(filesystem.write_file(path, data, parents=parents))
