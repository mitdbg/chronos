"""Runtime operations exposed by the Chronos core MCP server."""

from __future__ import annotations

import base64
import dataclasses
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from chronos_core.branching import (
    ChronosBranchContext,
    MergeResolution,
)
from chronos_core.mcp.config import ChronosMcpConfig, StoreConfig
from chronos_core.workspace import (
    ChronosDuckDBStore,
    ChronosFSStore,
    ChronosPostgresStore,
    ChronosWorkspaceContext,
)
from chronos_core.workspace.chronosfs import start_chronosfs_mount


class ChronosMcpRuntimeError(RuntimeError):
    """Raised for invalid MCP operations before converting to JSON errors."""


class ChronosMcpRuntime:
    """Branch-aware control plane for Codex-facing Chronos MCP tools."""

    def __init__(self, config: ChronosMcpConfig):
        self.config = config
        self.filesystem = self._open_filesystem()
        self.stores = self._open_stores()
        self.workspace = ChronosWorkspaceContext(
            filesystem=self.filesystem,
            **self.stores,
        )
        self._active_mounts: dict[str, Path] = {}

    def close(self) -> None:
        errors: list[Exception] = []
        for branch_id in list(self._active_mounts):
            try:
                self.unmount_branch(branch_id)
            except Exception as exc:
                errors.append(exc)
        try:
            self.workspace.close()
        except Exception as exc:
            errors.append(exc)
        if errors:
            raise errors[0]

    def status(self) -> dict[str, Any]:
        return self._ok(
            workspace={
                "name": self.config.workspace.name,
                "default_branch": self.config.workspace.default_branch,
                "state_dir": str(self.config.workspace.state_dir),
                "mount_root": str(self.config.workspace.mount_root),
            },
            stores=self._store_status(),
            active_mounts={
                branch: str(path)
                for branch, path in sorted(self._active_mounts.items())
            },
        )

    def prepare_source(self, source_dir: str, branch_id: str | None = None) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            fs = self._require_filesystem()
            branch = self._require_branch_id(branch_id or self.config.workspace.default_branch)
            source = self._validate_source_dir(source_dir)
            existing = fs.listdir(branch, "/")
            if existing:
                raise ChronosMcpRuntimeError(
                    f"filesystem branch '{branch}' is not empty; refusing source import"
                )
            fs.import_tree(branch, source)
            return self._ok(branch_id=branch, source_dir=str(source))

        return self._capture(op)

    def create_sandbox(
        self,
        branch_id: str,
        from_branch: str | None = None,
        *,
        mount: bool = True,
    ) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            branch = self._require_branch_id(branch_id)
            parent = self._require_branch_id(from_branch or self.config.workspace.default_branch)
            self.workspace.create_branch(branch, from_branch=parent)
            payload: dict[str, Any] = {"branch_id": branch, "from_branch": parent}
            if mount:
                mounted = self.mount_branch(branch)
                if not mounted.get("ok"):
                    raise ChronosMcpRuntimeError(str(mounted.get("error", {}).get("message", mounted)))
                payload["mount_path"] = mounted["mount_path"]
            return self._ok(**payload)

        return self._capture(op)

    def mount_branch(self, branch_id: str, mount_path: str | None = None) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            fs = self._require_filesystem()
            branch = self._require_branch_id(branch_id)
            path = Path(mount_path).expanduser() if mount_path else self._default_mount_path(branch)
            if not path.is_absolute():
                path = (self.config.project_dir / path).resolve()
            else:
                path = path.resolve()
            active = self._active_mounts.get(branch)
            if active is not None and active == path and os.path.ismount(path):
                return self._ok(branch_id=branch, mount_path=str(path), already_mounted=True)
            path.parent.mkdir(parents=True, exist_ok=True)
            start_chronosfs_mount(fs, path, branch_id=branch)
            self._active_mounts[branch] = path
            return self._ok(branch_id=branch, mount_path=str(path), already_mounted=False)

        return self._capture(op)

    def unmount_branch(self, branch_id: str) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            branch = self._require_branch_id(branch_id)
            path = self._active_mounts.pop(branch, None)
            if path is None:
                return self._ok(branch_id=branch, mounted=False)
            self._unmount_path(path)
            return self._ok(branch_id=branch, mount_path=str(path), mounted=False)

        return self._capture(op)

    def delete_branch(self, branch_id: str, *, unmount: bool = True) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            branch = self._require_branch_id(branch_id)
            if unmount:
                result = self.unmount_branch(branch)
                if not result.get("ok"):
                    raise ChronosMcpRuntimeError(str(result.get("error", {}).get("message", result)))
            self.workspace.delete_branch(branch)
            return self._ok(branch_id=branch, deleted=True)

        return self._capture(op)

    def sql_query(
        self,
        branch_id: str,
        store: str,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        limit: int = 1000,
    ) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            if limit < 0:
                raise ChronosMcpRuntimeError("limit must be non-negative")
            session = self._store_session(branch_id, store)
            rows = session.query(sql, self._params(params))
            return self._ok(branch_id=branch_id, store=store, rows=_jsonable(rows[:limit]))

        return self._capture(op)

    def sql_execute(
        self,
        branch_id: str,
        store: str,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            sql_text = self._require_sql(sql)
            if _is_ddl(sql_text):
                rowcount = self._execute_store_ddl(store, sql_text, self._params(params))
            else:
                result = self._store_session(branch_id, store).execute(sql_text, self._params(params))
                rowcount = int(getattr(result, "rowcount", 0))
            return self._ok(branch_id=branch_id, store=store, rowcount=rowcount)

        return self._capture(op)

    def register_table(self, store: str, table: str, primary_key: list[str]) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            if not primary_key or not all(isinstance(item, str) and item for item in primary_key):
                raise ChronosMcpRuntimeError("primary_key must be a non-empty list of strings")
            target = self._store_by_name(store)
            register = getattr(target, "register_table", None)
            if not callable(register):
                raise ChronosMcpRuntimeError(f"store does not support table registration: {store}")
            register(table, list(primary_key))
            return self._ok(store=store, table=table, primary_key=list(primary_key))

        return self._capture(op)

    def diff(self, left: str, right: str) -> dict[str, Any]:
        return self._capture(
            lambda: self._ok(left=left, right=right, diff=_jsonable(self.workspace.diff(left, right)))
        )

    def merge_preview(
        self,
        source: str,
        target: str | None = None,
        *,
        policy: Any = "manual_review",
    ) -> dict[str, Any]:
        return self._capture(
            lambda: self._ok(
                source=source,
                target=target or self.config.workspace.default_branch,
                preview=_jsonable(
                    self.workspace.merge_preview(
                        source,
                        target or self.config.workspace.default_branch,
                        policy=policy,
                    )
                ),
            )
        )

    def merge_apply(
        self,
        source: str,
        target: str | None = None,
        *,
        policy: Any = "snapshot_isolation",
        resolution: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            target_branch = target or self.config.workspace.default_branch
            result = self.workspace.merge_apply(
                source,
                target_branch,
                self._parse_resolution(resolution),
                policy=policy,
            )
            return self._ok(source=source, target=target_branch, result=_jsonable(result))

        return self._capture(op)

    def checkpoint(self, branch_id: str, checkpoint: str) -> dict[str, Any]:
        return self._capture(
            lambda: self._ok(
                branch_id=branch_id,
                checkpoint=checkpoint,
                result=_jsonable(self.workspace.create_checkpoint(checkpoint, branch=branch_id)),
            )
        )

    def _open_filesystem(self) -> ChronosFSStore | None:
        fs_config = self.config.filesystem
        if fs_config is None:
            return None
        store = ChronosFSStore.connect(
            fs_config.database_url,
            backend="interval",
            block_size=fs_config.block_size,
        )
        store.ensure()
        return store

    def _open_stores(self) -> dict[str, Any]:
        stores: dict[str, Any] = {}
        for name, store_config in self.config.stores.items():
            store = self._open_store(store_config)
            for table in store_config.tables:
                store.register_table(table.name, list(table.primary_key))
            stores[name] = store
        return stores

    def _open_store(self, config: StoreConfig) -> Any:
        if config.kind == "sqlite":
            if config.database_url is None:
                raise ChronosMcpRuntimeError(f"sqlite store is missing database_url: {config.name}")
            return ChronosBranchContext.connect(config.database_url, backend="interval")
        if config.kind == "postgresql":
            if config.data_url is None:
                raise ChronosMcpRuntimeError(f"postgresql store is missing data_url: {config.name}")
            return ChronosPostgresStore(config.data_url, metadata_url=config.metadata_url)
        if config.kind == "duckdb":
            if config.data_url is None or config.metadata_url is None:
                raise ChronosMcpRuntimeError(f"duckdb store requires data_url and metadata_url: {config.name}")
            return ChronosDuckDBStore(config.data_url, config.metadata_url)
        raise ChronosMcpRuntimeError(f"unsupported store kind: {config.kind}")

    def _store_status(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.filesystem is not None:
            result["filesystem"] = {
                "kind": "filesystem",
                "branches": list(self.filesystem.branches),
            }
        for name, store in self.stores.items():
            context = _store_context(store)
            result[name] = {
                "kind": self.config.stores[name].kind,
                "branches": _jsonable(context.list_branches()),
                "tables": [
                    {"name": table.name, "primary_key": list(table.primary_key)}
                    for table in self.config.stores[name].tables
                ],
            }
        return result

    def _store_session(self, branch_id: str, store: str) -> Any:
        branch = self._require_branch_id(branch_id)
        session = self.workspace.checkout(branch)
        try:
            return getattr(session, store)
        except AttributeError as exc:
            raise ChronosMcpRuntimeError(f"unknown SQL store: {store}") from exc

    def _execute_store_ddl(self, store: str, sql: str, params: dict[str, Any]) -> int:
        target = self._store_by_name(store)
        context = _store_context(target)
        cursor = context.db.execute(sql, params)
        context.db.commit()
        backend = getattr(context, "_backend", None)
        after_commit = getattr(backend, "after_commit", None)
        if callable(after_commit):
            after_commit()
        refresh = getattr(backend, "refresh_registries", None)
        if callable(refresh):
            refresh()
        return int(getattr(cursor, "rowcount", 0) or 0)

    def _store_by_name(self, store: str) -> Any:
        try:
            return self.stores[store]
        except KeyError as exc:
            raise ChronosMcpRuntimeError(f"unknown SQL store: {store}") from exc

    def _require_filesystem(self) -> ChronosFSStore:
        if self.filesystem is None:
            raise ChronosMcpRuntimeError("filesystem store is not configured")
        return self.filesystem

    def _validate_source_dir(self, source_dir: str) -> Path:
        source = Path(source_dir).expanduser()
        if not source.is_absolute():
            source = self.config.project_dir / source
        source = source.resolve()
        if not source.is_dir():
            raise ChronosMcpRuntimeError(f"source_dir is not a directory: {source}")
        roots = self.config.workspace.allowed_source_roots
        if not any(_is_relative_to(source, root) for root in roots):
            allowed = ", ".join(str(root) for root in roots)
            raise ChronosMcpRuntimeError(
                f"source_dir must be under an allowed source root: {allowed}"
            )
        return source

    def _default_mount_path(self, branch_id: str) -> Path:
        return (self.config.workspace.mount_root / _safe_mount_name(branch_id)).resolve()

    def _unmount_path(self, path: Path) -> None:
        if not os.path.ismount(path):
            return
        commands = (
            ("fusermount3", "-u", str(path)),
            ("fusermount", "-u", str(path)),
            ("umount", str(path)),
        )
        last_error: Exception | None = None
        for cmd in commands:
            if shutil.which(cmd[0]) is None:
                continue
            try:
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                return
            except Exception as exc:  # pragma: no cover - host FUSE dependent.
                last_error = exc
        if last_error is not None:
            raise ChronosMcpRuntimeError(f"failed to unmount {path}: {last_error}")
        raise ChronosMcpRuntimeError("no unmount command found (fusermount3, fusermount, umount)")

    @staticmethod
    def _params(params: dict[str, Any] | None) -> dict[str, Any]:
        if params is None:
            return {}
        if not isinstance(params, dict):
            raise ChronosMcpRuntimeError("params must be a JSON object of named SQL parameters")
        return params

    @staticmethod
    def _require_sql(sql: str) -> str:
        if not isinstance(sql, str) or not sql.strip():
            raise ChronosMcpRuntimeError("sql must be a non-empty string")
        return sql

    @staticmethod
    def _require_branch_id(branch_id: str | None) -> str:
        if not isinstance(branch_id, str) or not branch_id.strip():
            raise ChronosMcpRuntimeError("branch_id must be a non-empty string")
        return branch_id.strip()

    @staticmethod
    def _parse_resolution(value: dict[str, Any] | None) -> MergeResolution | dict[str, MergeResolution] | None:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ChronosMcpRuntimeError("resolution must be a JSON object")
        if "conflict_choices" in value:
            choices = value.get("conflict_choices")
            if not isinstance(choices, dict):
                raise ChronosMcpRuntimeError("resolution.conflict_choices must be an object")
            return MergeResolution({str(k): str(v) for k, v in choices.items()})
        parsed: dict[str, MergeResolution] = {}
        for store, raw in value.items():
            if not isinstance(raw, dict):
                raise ChronosMcpRuntimeError("store-specific resolutions must be objects")
            choices = raw.get("conflict_choices", {})
            if not isinstance(choices, dict):
                raise ChronosMcpRuntimeError("store-specific conflict_choices must be objects")
            parsed[str(store)] = MergeResolution({str(k): str(v) for k, v in choices.items()})
        return parsed

    @staticmethod
    def _ok(**payload: Any) -> dict[str, Any]:
        return {"ok": True, **payload}

    @staticmethod
    def _capture(fn: Any) -> dict[str, Any]:
        try:
            return fn()
        except Exception as exc:
            return {
                "ok": False,
                "error": {
                    "type": exc.__class__.__name__,
                    "message": str(exc),
                },
            }


def _store_context(store: Any) -> ChronosBranchContext:
    return getattr(store, "context", store)


def _is_ddl(sql: str) -> bool:
    return sql.lstrip().split(None, 1)[0].upper() in {
        "ALTER",
        "CREATE",
        "DROP",
        "TRUNCATE",
    }


def _safe_mount_name(branch_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", branch_id).strip("._")
    return safe or "branch"


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _jsonable(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, bytes):
        return {
            "encoding": "base64",
            "data": base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
