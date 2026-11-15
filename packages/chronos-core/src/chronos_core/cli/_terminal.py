"""Shared terminal output, error reporting, and CLI policy choices."""
from __future__ import annotations

import base64
import json
import re
import sys
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, NoReturn
from urllib.parse import parse_qsl, unquote, urlsplit

import typer


class MergePolicy(str, Enum):
    api_default = "api-default"
    abort_on_conflict = "abort_on_conflict"
    snapshot_isolation = "snapshot_isolation"

    @property
    def api_value(self) -> str | None:
        return None if self is MergePolicy.api_default else self.value


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


def read_json_input(text: str | None, file: Path | None, stdin: bool) -> Any:
    if sum((text is not None, file is not None, stdin)) > 1:
        raise typer.BadParameter("Choose JSON text, --file, or --stdin, not multiple sources.")
    try:
        if file is not None:
            text = file.read_text(encoding="utf-8")
        elif stdin or (text is None and not sys.stdin.isatty()):
            text = sys.stdin.read()
    except (OSError, UnicodeError) as error:
        raise typer.BadParameter(f"Cannot read JSON input: {error}") from error
    if text is None or not text.strip():
        raise typer.BadParameter("Provide JSON text, --file, or JSON on stdin.")
    try:
        return json.loads(text, parse_constant=_reject_constant)
    except ValueError as error:
        raise typer.BadParameter(f"Invalid JSON: {error}") from error


def _reject_constant(value: str) -> None:
    raise ValueError(f"{value} is not a finite JSON number")


def json_object_option(value: str | None, param_hint: str) -> dict[str, Any] | None:
    if value is None:
        return None
    try:
        result = json.loads(value, parse_constant=_reject_constant)
    except ValueError as error:
        raise typer.BadParameter("Expected a JSON object.", param_hint=param_hint) from error
    if not isinstance(result, dict):
        raise typer.BadParameter("Expected a JSON object.", param_hint=param_hint)
    return result


def print_result(result: Any) -> None:
    if result is not None:
        typer.echo(json.dumps(result, default=_json_default, ensure_ascii=False, indent=2, allow_nan=False))


def report_error(
    error: Exception, *, secrets: tuple[str | None, ...] = (), urls: tuple[str | None, ...] = (),
) -> NoReturn:
    """Report an operational failure without exposing configured credentials."""
    message = str(error)
    sensitive = [secret for secret in secrets if secret]
    for url in urls:
        if url:
            message = message.replace(url, "<database>")
            try:
                parsed = urlsplit(url)
            except ValueError:
                continue
            if parsed.password:
                sensitive.extend((parsed.password, unquote(parsed.password)))
            sensitive.extend(value for key, value in parse_qsl(parsed.query) if key == "password" and value)
            for item in parsed.query.split("&"):
                key, separator, value = item.partition("=")
                if separator and unquote(key) == "password" and value:
                    sensitive.append(value)
    for secret in sorted(set(sensitive), key=len, reverse=True):
        message = message.replace(secret, "<redacted>")
    message = re.sub(r"([a-zA-Z][\w+.-]*://)[^\s/@]+@", r"\1<credentials>@", message)
    message = re.sub(r"(?i)(password\s*=\s*)(?:'[^']*'|\"[^\"]*\"|[^\s]+)", r"\1<redacted>", message)
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(1) from error
