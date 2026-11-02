"""Shared terminal output, error reporting, and CLI policy choices."""
from __future__ import annotations

import base64
import json
import re
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, NoReturn

import typer


class MergePolicy(str, Enum):
    abort_on_conflict = "abort_on_conflict"
    snapshot_isolation = "snapshot_isolation"


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
