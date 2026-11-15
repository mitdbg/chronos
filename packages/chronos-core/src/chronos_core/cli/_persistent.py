"""Persistent mode: run ordinary CLI commands against one open workspace.

Each stdin line is one command, split like a shell would split it and run by the
same Click command tree as a one-shot invocation. Each command produces one
JSON line on stdout wrapping what the one-shot command would have printed.
"""
from __future__ import annotations

import base64
import io
import json
import shlex
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import IO, Any

import typer

from chronos_core.cli._terminal import report_error
from chronos_core.cli._workspace import held_workspace, hold_workspace


# Usage errors (exit code 2) share a base class named ClickException. typer < 0.29
# takes it from click and typer >= 0.29 bundles its own copy, so find it via typer.
_UsageErrorBase = next(cls for cls in typer.BadParameter.__mro__ if cls.__name__ == "ClickException")


@contextmanager
def _captured_streams() -> Iterator[tuple[io.BytesIO, io.BytesIO]]:
    """Give one command empty stdin and byte-backed stdout/stderr, like a fresh process."""
    out, err = io.BytesIO(), io.BytesIO()
    saved = sys.stdin, sys.stdout, sys.stderr
    streams = (
        io.TextIOWrapper(io.BytesIO(), encoding="utf-8"),
        io.TextIOWrapper(out, encoding="utf-8", write_through=True),
        io.TextIOWrapper(err, encoding="utf-8", write_through=True),
    )
    sys.stdin, sys.stdout, sys.stderr = streams
    try:
        yield out, err
    finally:
        sys.stdin, sys.stdout, sys.stderr = saved
        for stream in streams:
            stream.flush()
            # Detach so discarding the wrapper does not close the captured bytes.
            stream.detach()


def _output_value(data: bytes) -> tuple[Any, Any]:
    """Return (result, output): parsed JSON, or the raw output when it is not JSON."""
    if not data.strip():
        return None, None
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None, {"encoding": "base64", "data": base64.b64encode(data).decode("ascii")}
    try:
        return json.loads(text), None
    except ValueError:
        return None, text


def run_line(command: Any, line: str) -> dict[str, Any]:
    try:
        args = shlex.split(line)
    except ValueError as error:
        return {"ok": False, "exit_code": 2, "result": None, "output": None, "error": f"Error: {error}"}
    if args[:2] == ["fs", "mount"]:
        return {"ok": False, "exit_code": 2, "result": None, "output": None,
                "error": "Error: fs mount runs in the foreground; use a one-shot command."}
    with _captured_streams() as (out, err):
        try:
            code = command.main(args, prog_name="chronos", standalone_mode=False)
            code = code if isinstance(code, int) else 0
        except _UsageErrorBase as error:
            error.show()
            code = error.exit_code
        except typer.Abort:
            typer.echo("Aborted!", err=True)
            code = 1
        except Exception as error:
            # A one-shot process would end with a traceback; keep serving instead.
            held = held_workspace()
            try:
                (held.policy.report if held is not None else report_error)(error)
            except typer.Exit:
                pass
            code = 1
    result, output = _output_value(out.getvalue())
    message = err.getvalue().decode("utf-8", errors="replace").strip()
    return {"ok": code == 0, "exit_code": code, "result": result, "output": output, "error": message or None}


def run_persistent(ctx: typer.Context, command: Any,
                   stdin: IO[str] | None = None, stdout: IO[str] | None = None) -> None:
    source = stdin or sys.stdin
    sink = stdout or sys.stdout
    with hold_workspace(ctx):
        for line in iter(source.readline, ""):
            if not line.strip():
                continue
            response = run_line(command, line)
            sink.write(json.dumps(response, ensure_ascii=False, allow_nan=False) + "\n")
            sink.flush()
