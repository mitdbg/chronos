"""Configuration loading for the Chronos core MCP server."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.10.
    import tomli as tomllib  # type: ignore[no-redef]


StoreKind = Literal["sqlite", "postgresql", "duckdb"]


class ChronosMcpConfigError(ValueError):
    """Raised when ``chronos.toml`` cannot be loaded as an MCP config."""


@dataclass(frozen=True)
class TableConfig:
    name: str
    primary_key: tuple[str, ...]


@dataclass(frozen=True)
class StoreConfig:
    name: str
    kind: StoreKind
    database_url: str | None = None
    data_url: str | None = None
    metadata_url: str | None = None
    tables: tuple[TableConfig, ...] = ()


@dataclass(frozen=True)
class FilesystemConfig:
    database_url: str
    block_size: int = 4096


@dataclass(frozen=True)
class WorkspaceConfig:
    name: str
    default_branch: str
    state_dir: Path
    mount_root: Path
    allowed_source_roots: tuple[Path, ...]


@dataclass(frozen=True)
class ChronosMcpConfig:
    config_path: Path
    project_dir: Path
    workspace: WorkspaceConfig
    filesystem: FilesystemConfig | None = None
    stores: dict[str, StoreConfig] = field(default_factory=dict)


def load_chronos_mcp_config(path: str | Path) -> ChronosMcpConfig:
    """Load and validate a ``chronos.toml`` file for the MCP server."""
    config_path = Path(path).expanduser().resolve()
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ChronosMcpConfigError(f"config file does not exist: {config_path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ChronosMcpConfigError(f"invalid TOML in {config_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ChronosMcpConfigError("config root must be a TOML table")

    project_dir = config_path.parent
    workspace = _parse_workspace(raw.get("workspace"), project_dir)
    filesystem = _parse_filesystem(raw.get("filesystem"), project_dir)
    stores = _parse_stores(raw.get("stores"), project_dir)
    if filesystem is None and not stores:
        raise ChronosMcpConfigError("at least one filesystem or SQL store must be configured")
    return ChronosMcpConfig(
        config_path=config_path,
        project_dir=project_dir,
        workspace=workspace,
        filesystem=filesystem,
        stores=stores,
    )


def _parse_workspace(value: Any, project_dir: Path) -> WorkspaceConfig:
    data = _expect_table(value, "workspace", default={})
    name = _expect_str(data.get("name", project_dir.name), "workspace.name")
    default_branch = _expect_str(data.get("default_branch", "main"), "workspace.default_branch")
    state_dir = _resolve_path(data.get("state_dir", ".chronos"), project_dir, "workspace.state_dir")
    mount_root = _resolve_path(
        data.get("mount_root", str(state_dir / "mounts")),
        project_dir,
        "workspace.mount_root",
    )
    raw_roots = data.get("allowed_source_roots", ["."])
    if not isinstance(raw_roots, list) or not raw_roots:
        raise ChronosMcpConfigError("workspace.allowed_source_roots must be a non-empty list")
    roots = tuple(
        _resolve_path(item, project_dir, "workspace.allowed_source_roots")
        for item in raw_roots
    )
    return WorkspaceConfig(
        name=name,
        default_branch=default_branch,
        state_dir=state_dir,
        mount_root=mount_root,
        allowed_source_roots=roots,
    )


def _parse_filesystem(value: Any, project_dir: Path) -> FilesystemConfig | None:
    if value is None:
        return None
    data = _expect_table(value, "filesystem")
    database_url = _normalize_local_url(
        _expect_str(data.get("database_url"), "filesystem.database_url"),
        project_dir,
        expected_kind="sqlite",
    )
    block_size = _expect_int(data.get("block_size", 4096), "filesystem.block_size")
    if block_size <= 0:
        raise ChronosMcpConfigError("filesystem.block_size must be positive")
    return FilesystemConfig(database_url=database_url, block_size=block_size)


def _parse_stores(value: Any, project_dir: Path) -> dict[str, StoreConfig]:
    if value is None:
        return {}
    stores = _expect_table(value, "stores")
    result: dict[str, StoreConfig] = {}
    for name, raw_store in stores.items():
        if not isinstance(name, str) or not name:
            raise ChronosMcpConfigError("store names must be non-empty strings")
        data = _expect_table(raw_store, f"stores.{name}")
        kind = _expect_str(data.get("kind"), f"stores.{name}.kind")
        if kind not in ("sqlite", "postgresql", "duckdb"):
            raise ChronosMcpConfigError(
                f"stores.{name}.kind must be one of sqlite, postgresql, duckdb"
            )
        tables = _parse_tables(data.get("tables", []), f"stores.{name}.tables")
        if kind == "sqlite":
            database_url = _normalize_local_url(
                _expect_str(data.get("database_url"), f"stores.{name}.database_url"),
                project_dir,
                expected_kind="sqlite",
            )
            result[name] = StoreConfig(
                name=name,
                kind="sqlite",
                database_url=database_url,
                tables=tables,
            )
        elif kind == "postgresql":
            data_url = _expect_str(data.get("data_url"), f"stores.{name}.data_url")
            result[name] = StoreConfig(
                name=name,
                kind="postgresql",
                data_url=data_url,
                metadata_url=_optional_str(data.get("metadata_url"), f"stores.{name}.metadata_url"),
                tables=tables,
            )
        else:
            data_url = _normalize_duckdb_url(
                _expect_str(data.get("data_url"), f"stores.{name}.data_url"),
                project_dir,
            )
            metadata_url = _normalize_local_url(
                _expect_str(data.get("metadata_url"), f"stores.{name}.metadata_url"),
                project_dir,
                expected_kind="sqlite",
            )
            result[name] = StoreConfig(
                name=name,
                kind="duckdb",
                data_url=data_url,
                metadata_url=metadata_url,
                tables=tables,
            )
    return result


def _parse_tables(value: Any, field_name: str) -> tuple[TableConfig, ...]:
    if not isinstance(value, list):
        raise ChronosMcpConfigError(f"{field_name} must be a list")
    tables: list[TableConfig] = []
    for idx, raw_table in enumerate(value):
        table_field = f"{field_name}[{idx}]"
        data = _expect_table(raw_table, table_field)
        name = _expect_str(data.get("name"), f"{table_field}.name")
        primary_key = data.get("primary_key")
        if (
            not isinstance(primary_key, list)
            or not primary_key
            or not all(isinstance(item, str) and item for item in primary_key)
        ):
            raise ChronosMcpConfigError(f"{table_field}.primary_key must be a non-empty string list")
        tables.append(TableConfig(name=name, primary_key=tuple(primary_key)))
    return tuple(tables)


def _normalize_duckdb_url(value: str, project_dir: Path) -> str:
    parsed = urlparse(value)
    if parsed.scheme in ("postgres", "postgresql", "sqlite"):
        raise ChronosMcpConfigError("DuckDB data_url must be a DuckDB URL or local file path")
    if parsed.scheme == "duckdb":
        return _normalize_local_url(value, project_dir, expected_kind="duckdb")
    if parsed.scheme:
        raise ChronosMcpConfigError(f"unsupported DuckDB data_url scheme: {parsed.scheme}")
    path = _resolve_path(value, project_dir, "duckdb.data_url")
    return _local_file_url("duckdb", path)


def _normalize_local_url(value: str, project_dir: Path, *, expected_kind: str) -> str:
    if value in (":memory:", f"{expected_kind}:///:memory:"):
        return f"{expected_kind}:///:memory:" if expected_kind == "duckdb" else value
    parsed = urlparse(value)
    if parsed.scheme == "":
        path = _resolve_path(value, project_dir, f"{expected_kind}.database_url")
        return _local_file_url(expected_kind, path)
    if parsed.scheme != expected_kind:
        raise ChronosMcpConfigError(
            f"expected {expected_kind} URL, got scheme {parsed.scheme!r}: {value}"
        )
    if parsed.netloc and parsed.netloc not in ("", "localhost"):
        raise ChronosMcpConfigError(f"{expected_kind} URL must be local: {value}")
    if value.startswith(f"{expected_kind}:////"):
        return value
    if value.startswith(f"{expected_kind}:///"):
        rel = parsed.path.lstrip("/")
        if rel.startswith("."):
            return _local_file_url(expected_kind, (project_dir / rel).resolve())
    return value


def _local_file_url(kind: str, path: Path) -> str:
    return f"{kind}:///{path.resolve()}"


def _resolve_path(value: Any, project_dir: Path, field_name: str) -> Path:
    raw = _expect_str(value, field_name)
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = project_dir / path
    return path.resolve()


def _expect_table(value: Any, field_name: str, *, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if value is None and default is not None:
        return default
    if not isinstance(value, dict):
        raise ChronosMcpConfigError(f"{field_name} must be a table")
    return value


def _expect_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ChronosMcpConfigError(f"{field_name} must be a non-empty string")
    return value


def _optional_str(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _expect_str(value, field_name)


def _expect_int(value: Any, field_name: str) -> int:
    if not isinstance(value, int):
        raise ChronosMcpConfigError(f"{field_name} must be an integer")
    return value
