"""Qdrant configuration, JSON input, and ownership of public API resources."""
from __future__ import annotations

import json
import math
import sqlite3
import sys
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
import typer

from chronos_core.cli._support import report_error

if TYPE_CHECKING:
    from chronos_core.workspace.qdrant import ChronosQdrantStore, QdrantBranchSession


@dataclass(frozen=True)
class QdrantConfig:
    metadata_url: str
    path: Path | None = None
    url: str | None = None
    api_key: str | None = None
    collection_prefix: str = "chronos_"
    timeout: float | None = None


def qdrant_config(ctx: typer.Context) -> QdrantConfig:
    config = ctx.find_object(QdrantConfig)
    if config is None or not config.metadata_url:
        raise typer.BadParameter("Provide --metadata-url or CHRONOS_METADATA_URL.", param_hint="--metadata-url")
    if (config.path is None) == (config.url is None):
        raise typer.BadParameter("Choose exactly one of --path or --url.")
    if config.metadata_url in {":memory:", "sqlite:///:memory:"} or str(config.path) == ":memory:":
        raise typer.BadParameter("One-off commands require persistent storage, not :memory:.")
    if config.path is not None and (config.api_key is not None or config.timeout is not None):
        raise typer.BadParameter("--api-key and --timeout require --url.")
    return config


def _store_class() -> type[ChronosQdrantStore]:
    # Optional dependencies must not be imported when showing help or using SQL.
    from chronos_core.workspace.qdrant import ChronosQdrantStore
    return ChronosQdrantStore


@contextmanager
def open_qdrant(config: QdrantConfig) -> Generator[ChronosQdrantStore, None, None]:
    try:
        import grpc
        import httpx
        from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse
        from chronos_core.branching import BranchingError
    except ImportError:
        report_error(RuntimeError("Qdrant support requires installing chronos-core[qdrant]."))

    try:
        factory = _store_class()
        if config.path is not None:
            store = factory.local(
                config.metadata_url, path=config.path, collection_prefix=config.collection_prefix,
            )
        elif config.url is not None:
            store = factory.remote(
                config.metadata_url, url=config.url, api_key=config.api_key,
                collection_prefix=config.collection_prefix, timeout=config.timeout,
            )
        else:
            raise ValueError("Choose exactly one of --path or --url.")
        try:
            yield store
        finally:
            store.close()
    except (
        BranchingError, RuntimeError, ValueError, TypeError, OSError, sqlite3.Error,
        psycopg.Error, httpx.HTTPError, grpc.RpcError, ResponseHandlingException, UnexpectedResponse,
    ) as error:
        report_error(error, secrets=(config.api_key,), urls=(config.metadata_url, config.url))


@contextmanager
def qdrant_session(store: ChronosQdrantStore, branch: str) -> Generator[QdrantBranchSession, None, None]:
    # Qdrant sessions wrap a relational control session but have no close method.
    # Both of these are public APIs; keep control-session ownership explicit.
    with store.context.checkout(branch) as control:
        yield store.checkout_control(control)


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


def dense_vector(value: Any) -> list[float]:
    try:
        valid = isinstance(value, list) and all(
            isinstance(item, (int, float)) and not isinstance(item, bool) and math.isfinite(item)
            for item in value
        )
    except OverflowError:
        valid = False
    if not valid:
        raise typer.BadParameter("Expected a JSON array of finite numbers for the vector.")
    return value


def point_input(value: Any) -> tuple[list[float], dict[str, Any] | None]:
    if not isinstance(value, dict) or "vector" not in value or set(value) - {"vector", "payload"}:
        raise typer.BadParameter("Expected an object with vector and optional payload fields.")
    payload = value.get("payload")
    if payload is not None and not isinstance(payload, dict):
        raise typer.BadParameter("The payload must be a JSON object or null.")
    return dense_vector(value["vector"]), payload
