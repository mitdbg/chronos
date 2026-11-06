"""Workspace configuration and ownership of public store resources."""
from __future__ import annotations

import math
import os
import re
import sys
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import unquote, urlsplit

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib  # type: ignore[no-redef]

if TYPE_CHECKING:
    from chronos_core.workspace import ChronosWorkspaceContext


@dataclass(frozen=True)
class DatabaseConfig:
    database_url: str
    metadata_url: str | None = None


@dataclass(frozen=True)
class QdrantConfig:
    metadata_url: str
    path: Path | None = None
    url: str | None = None
    api_key: str | None = None
    collection_prefix: str = "chronos_"
    timeout: float | None = None


class WorkspaceConfigError(ValueError):
    """Invalid workspace configuration, before connecting to any store."""


@dataclass(frozen=True)
class StoreConfig:
    kind: Literal["sqlite", "postgresql", "duckdb", "qdrant"]
    connection: DatabaseConfig | QdrantConfig

    @property
    def metadata_url(self) -> str:
        connection = self.connection
        if isinstance(connection, QdrantConfig):
            return connection.metadata_url
        return connection.metadata_url or connection.database_url


@dataclass(frozen=True)
class FilesystemConfig:
    root: Path
    state_dir: Path


@dataclass(frozen=True)
class WorkspaceConfig:
    config_path: Path
    stores: dict[str, StoreConfig]
    shared_metadata_url: str | None = None
    filesystem: FilesystemConfig | None = None

    def store(self, name: str, *, qdrant: bool = False) -> StoreConfig:
        store = self.stores.get(name)
        if store is None:
            raise WorkspaceConfigError(f"Unknown store {name!r}.")
        if (store.kind == "qdrant") != qdrant:
            expected = "Qdrant" if qdrant else "relational"
            raise WorkspaceConfigError(f"Store {name!r} is not a {expected} store.")
        return store


def _table(value: Any, field: str, allowed: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise WorkspaceConfigError(f"{field} must be a TOML table.")
    if set(value) - allowed:
        raise WorkspaceConfigError(f"{field} contains unsupported fields.")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkspaceConfigError(f"{field} must be a non-empty string.")

    def substitute(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise WorkspaceConfigError(f"{field} references unset environment variable {name!r}.")
        return os.environ[name]

    result = re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", substitute, value)
    if not result.strip():
        raise WorkspaceConfigError(f"{field} must not resolve to an empty value.")
    return result


def _local_path(value: str | Path, root: Path, field: str) -> Path:
    try:
        location = Path(value).expanduser()
        return (location if location.is_absolute() else root / location).resolve()
    except (OSError, RuntimeError, ValueError) as error:
        raise WorkspaceConfigError(f"{field} must be a valid local path.") from error


def _metadata_identity(url: str, root: Path) -> str:
    if url.startswith("sqlite:///"):
        # Match the adapter's decoding for comparisons without rewriting the
        # URL passed to the API (literal percent signs may themselves be encoded).
        return f"sqlite:///{_local_path(unquote(url[len('sqlite:///'):]), root, 'metadata_url')}"
    return url


def _database_url(value: Any, root: Path, field: str, *, kind: str | None = None) -> str:
    url = _text(value, field)
    try:
        parsed = urlsplit(url)
    except ValueError as error:
        raise WorkspaceConfigError(f"{field} must be a valid database URL.") from error
    scheme = "postgresql" if parsed.scheme == "postgres" else parsed.scheme
    expected = {kind} if kind is not None else {"sqlite", "postgresql"}
    if scheme not in expected:
        raise WorkspaceConfigError(f"{field} has an unsupported database URL scheme.")
    if scheme == "postgresql":
        return "postgresql:" + url.split(":", 1)[1]
    if parsed.netloc or parsed.query or parsed.fragment or not url.startswith(f"{scheme}:///"):
        raise WorkspaceConfigError(f"{field} must be a local database file URL.")
    local = url[len(f"{scheme}:///"):]
    if not local or local == ":memory:":
        raise WorkspaceConfigError(f"{field} requires persistent storage, not an in-memory database.")
    return f"{scheme}:///{_local_path(local, root, field)}"


def _store_config(value: Any, root: Path, field: str) -> StoreConfig:
    if not isinstance(value, dict):
        raise WorkspaceConfigError(f"{field} must be a TOML table.")
    kind = value.get("kind")
    if kind in ("sqlite", "postgresql", "duckdb"):
        data = _table(value, field, {"kind", "database_url", "metadata_url"})
        url = _database_url(data.get("database_url"), root, f"{field}.database_url", kind=kind)
        metadata = (
            _database_url(data["metadata_url"], root, f"{field}.metadata_url")
            if "metadata_url" in data else None
        )
        if kind == "duckdb" and metadata is None:
            raise WorkspaceConfigError(f"{field}.metadata_url is required for DuckDB.")
        return StoreConfig(kind, DatabaseConfig(url, metadata))
    if kind != "qdrant":
        raise WorkspaceConfigError(f"{field}.kind must be sqlite, postgresql, duckdb, or qdrant.")
    data = _table(value, field, {"kind", "metadata_url", "path", "url", "api_key", "collection_prefix", "timeout"})
    metadata = _database_url(data.get("metadata_url"), root, f"{field}.metadata_url")
    if ("path" in data) == ("url" in data):
        raise WorkspaceConfigError(f"{field} requires exactly one of path or url.")
    local_path = None
    url = None
    if "path" in data:
        local = _text(data["path"], f"{field}.path")
        if local == ":memory:":
            raise WorkspaceConfigError(f"{field}.path requires persistent storage.")
        local_path = _local_path(local, root, f"{field}.path")
        if "api_key" in data or "timeout" in data:
            raise WorkspaceConfigError(f"{field}: api_key and timeout require url.")
    else:
        url = _text(data["url"], f"{field}.url")
        try:
            remote = urlsplit(url)
        except ValueError as error:
            raise WorkspaceConfigError(f"{field}.url must be a valid HTTP or HTTPS URL.") from error
        if remote.scheme not in {"http", "https"} or not remote.hostname:
            raise WorkspaceConfigError(f"{field}.url must use HTTP or HTTPS.")
    api_key = _text(data["api_key"], f"{field}.api_key") if "api_key" in data else None
    prefix = _text(data.get("collection_prefix", "chronos_"), f"{field}.collection_prefix")
    timeout = data.get("timeout")
    try:
        valid_timeout = timeout is None or (
            not isinstance(timeout, bool) and isinstance(timeout, (int, float))
            and math.isfinite(timeout) and timeout >= 0.001
        )
    except OverflowError:
        valid_timeout = False
    if not valid_timeout:
        raise WorkspaceConfigError(f"{field}.timeout must be a finite number of at least 0.001 seconds.")
    return StoreConfig("qdrant", QdrantConfig(metadata, local_path, url, api_key, prefix, timeout))


def load_workspace_config(path: Path | None = None) -> WorkspaceConfig:
    """Read explicit path, environment path, or ./chronos.toml without opening stores."""
    selected = path if path is not None else Path(os.environ.get("CHRONOS_WORKSPACE_CONFIG", "chronos.toml"))
    config_path = _local_path(selected, Path.cwd(), "--config")
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError) as error:
        raise WorkspaceConfigError(f"Cannot read workspace configuration: {config_path}") from error
    except tomllib.TOMLDecodeError as error:
        # Decoder messages can contain configuration values, including secrets.
        raise WorkspaceConfigError("Invalid TOML in workspace configuration.") from error
    data = _table(raw, "configuration", {"workspace", "stores", "filesystem"})
    workspace = _table(data.get("workspace", {}), "workspace", {"shared_metadata_url"})
    shared = (
        _database_url(workspace["shared_metadata_url"], config_path.parent, "workspace.shared_metadata_url")
        if "shared_metadata_url" in workspace else None
    )
    filesystem = None
    if "filesystem" in data:
        fields = _table(data["filesystem"], "filesystem", {"root", "state_dir"})
        filesystem = FilesystemConfig(*(
            _local_path(_text(fields.get(key), f"filesystem.{key}"), config_path.parent, f"filesystem.{key}")
            for key in ("root", "state_dir")
        ))
        if shared is not None:
            raise WorkspaceConfigError("The OverlayFS backend does not support shared-metadata atomic workspaces.")
    entries = data.get("stores", {})
    if not isinstance(entries, dict) or (not entries and filesystem is None):
        raise WorkspaceConfigError("Configure at least one named store in [stores.NAME] or [filesystem].")
    # These names cannot be used by WorkspaceBranchSession, even via its stores mapping.
    from chronos_core.workspace.runtime import WorkspaceBranchSession

    reserved = {"branch_id", "fs", "stores", "transaction", *dir(WorkspaceBranchSession)}
    stores = {}
    for name, value in entries.items():
        if not name.strip() or name in reserved or (filesystem is not None and name == "filesystem"):
            raise WorkspaceConfigError(f"Invalid or reserved workspace store name {name!r}.")
        stores[name] = _store_config(value, config_path.parent, f"stores.{name}")
    metadata_urls = [store.metadata_url for store in stores.values()]
    if shared is not None and any(url != shared for url in metadata_urls):
        raise WorkspaceConfigError("Every store must use workspace.shared_metadata_url for atomic operations.")
    if shared is not None and sum(store.kind != "qdrant" for store in stores.values()) > 1:
        raise WorkspaceConfigError(
            "This CLI does not yet support multiple SQL participants sharing metadata. "
            "Use independent metadata, or one SQL participant with Qdrant."
        )
    identities = [_metadata_identity(url, config_path.parent) for url in metadata_urls]
    if shared is None and len(set(identities)) != len(identities):
        raise WorkspaceConfigError("Stores sharing metadata require workspace.shared_metadata_url.")
    return WorkspaceConfig(config_path, stores, shared, filesystem)


@contextmanager
def open_workspace(config: WorkspaceConfig) -> Generator[ChronosWorkspaceContext, None, None]:
    """Close every owned resource; never remove persistent data or replace an API error."""
    from chronos_core.branching import ChronosBranchContext
    from chronos_core.workspace import ChronosWorkspaceContext

    errors: list[Exception] = []

    def close(resource: Any) -> None:
        try:
            resource.close()
        except Exception as error:
            errors.append(error)

    try:
        with ExitStack() as resources:
            stores: dict[str, Any] = {}
            contexts: dict[str, Any] = {}
            # Open SQL first so Qdrant may borrow an existing row-store context,
            # regardless of the order in which users declare their stores.
            for name, entry in config.stores.items():
                database = entry.connection
                if not isinstance(database, DatabaseConfig):
                    continue
                if database.metadata_url is None or database.metadata_url == database.database_url:
                    context = ChronosBranchContext.connect(database.database_url)
                    contexts[database.database_url] = context
                else:
                    context = ChronosBranchContext.connect_split(database.database_url, database.metadata_url)
                resources.callback(close, context)
                stores[name] = context
            for name, entry in config.stores.items():
                target = entry.connection
                if not isinstance(target, QdrantConfig):
                    continue
                from chronos_core.workspace.qdrant import ChronosQdrantStore

                factory = ChronosQdrantStore
                options = {"collection_prefix": target.collection_prefix, "context": contexts.get(target.metadata_url)}
                if target.path is not None:
                    store = factory.local(target.metadata_url, path=target.path, **options)
                else:
                    store = factory.remote(
                        target.metadata_url, url=target.url, api_key=target.api_key,
                        timeout=target.timeout, **options,
                    )
                resources.callback(close, store)
                stores[name] = store
            filesystem_options: dict[str, Any] = {}
            if config.filesystem is not None:
                from chronos_core.workspace import ChronosFilesystemStore

                filesystem = ChronosFilesystemStore(config.filesystem.root, state_dir=config.filesystem.state_dir)
                resources.callback(close, filesystem)
                filesystem_options["filesystem"] = filesystem
            workspace = ChronosWorkspaceContext(
                stores={name: stores[name] for name in config.stores},
                shared_metadata_url=config.shared_metadata_url,
                **filesystem_options,
            )
            # ExitStack owns the participants directly because workspace.close()
            # stops at the first close failure instead of attempting all stores.
            yield workspace
    finally:
        if errors and sys.exc_info()[0] is None:
            raise errors[0]
