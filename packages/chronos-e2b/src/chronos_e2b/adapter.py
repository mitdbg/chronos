"""E2B lifecycle adapter for the Chronos gateway."""

from __future__ import annotations

import ipaddress
import json
import os
import shlex
import urllib.error
import urllib.request
import uuid
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import quote

FilesystemAccess = Literal["none", "read_only", "read_write"]


class ChronosE2BError(RuntimeError):
    """Base exception raised by this adapter."""


class ChronosControlError(ChronosE2BError):
    """A Chronos gateway control request failed."""

    def __init__(self, status: int | None, detail: str):
        self.status = status
        self.detail = detail
        label = "transport error" if status is None else f"HTTP {status}"
        super().__init__(f"Chronos control request failed ({label}): {detail}")


class ChronosCleanupError(ChronosE2BError):
    """One or more sandbox cleanup operations failed."""

    def __init__(self, errors: Sequence[BaseException]):
        self.errors = tuple(errors)
        detail = "; ".join(str(error) for error in self.errors)
        super().__init__(f"Chronos sandbox cleanup failed: {detail}")


class _ControlClient:
    def __init__(self, url: str, token: str, timeout: float):
        if not url:
            raise ValueError("control_url must not be empty")
        if not token:
            raise ValueError("controller_token must not be empty")
        self.url = url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        request = urllib.request.Request(
            self.url + path,
            data=None if body is None else json.dumps(body).encode("utf-8"),
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                value = json.load(response)
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise ChronosControlError(error.code, detail) from error
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ChronosControlError(None, "gateway returned invalid JSON") from error
        except (OSError, urllib.error.URLError, TimeoutError) as error:
            raise ChronosControlError(None, str(error)) from error
        if not isinstance(value, dict):
            raise ChronosControlError(None, "gateway returned a non-object response")
        return value


class ChronosE2B:
    """Create branch-backed E2B sandboxes through a Chronos gateway."""

    def __init__(
        self,
        *,
        control_url: str,
        controller_token: str,
        sandbox_type: Any | None = None,
        request_timeout: float = 30,
    ):
        if sandbox_type is None:
            from e2b import Sandbox

            sandbox_type = Sandbox
        self._control = _ControlClient(
            control_url,
            controller_token,
            request_timeout,
        )
        self._sandbox_type = sandbox_type

    @classmethod
    def from_env(
        cls,
        *,
        sandbox_type: Any | None = None,
        request_timeout: float = 30,
    ) -> ChronosE2B:
        """Create an adapter from the coordinator's environment."""

        try:
            control_url = os.environ["CHRONOS_CONTROL_URL"]
            controller_token = os.environ["CHRONOS_CONTROLLER_TOKEN"]
        except KeyError as error:
            raise ChronosE2BError(
                f"missing required environment variable {error.args[0]}"
            ) from error
        return cls(
            control_url=control_url,
            controller_token=controller_token,
            sandbox_type=sandbox_type,
            request_timeout=request_timeout,
        )

    def branch(
        self,
        *,
        template: str,
        workspace: str,
        databases: Mapping[str, str] | Sequence[str] = (),
        from_branch: str = "main",
        filesystem: FilesystemAccess = "read_write",
        ttl_seconds: int = 3600,
        timeout: int | None = None,
        branch_id: str | None = None,
        mountpoint: str = "/mnt/chronos",
        envs: Mapping[str, str] | None = None,
        metadata: Mapping[str, str] | None = None,
        sandbox_kwargs: Mapping[str, Any] | None = None,
        prepare: Callable[[Any], None] | None = None,
    ) -> BranchSandbox:
        """Describe a sandbox; entering it creates and attaches the branch."""

        if not template:
            raise ValueError("template must not be empty")
        if not workspace:
            raise ValueError("workspace must not be empty")
        if not from_branch:
            raise ValueError("from_branch must not be empty")
        if filesystem not in {"none", "read_only", "read_write"}:
            raise ValueError("filesystem must be none, read_only, or read_write")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if not mountpoint.startswith("/") or mountpoint == "/":
            raise ValueError("mountpoint must be an absolute path other than /")

        if isinstance(databases, Mapping):
            database_envs = dict(databases)
        else:
            if isinstance(databases, str):
                raise ValueError("databases must be a mapping or a sequence of names")
            database_envs = {
                name: f"{name.upper().replace('-', '_')}_DATABASE_URL"
                for name in databases
            }
        if any(not name or not variable for name, variable in database_envs.items()):
            raise ValueError(
                "database names and environment variables must not be empty"
            )
        if len(set(database_envs.values())) != len(database_envs):
            raise ValueError("each database must use a distinct environment variable")

        reserved_kwargs = {"envs", "metadata", "timeout"}
        supplied_kwargs = dict(sandbox_kwargs or {})
        overlap = reserved_kwargs.intersection(supplied_kwargs)
        if overlap:
            names = ", ".join(sorted(overlap))
            raise ValueError(f"pass {names} through the corresponding branch arguments")

        return BranchSandbox(
            adapter=self,
            template=template,
            workspace=workspace,
            database_envs=database_envs,
            from_branch=from_branch,
            filesystem=filesystem,
            ttl_seconds=ttl_seconds,
            timeout=timeout if timeout is not None else ttl_seconds,
            branch_id=branch_id or f"rollout-{uuid.uuid4().hex}",
            mountpoint=mountpoint.rstrip("/"),
            envs=dict(envs or {}),
            metadata=dict(metadata or {}),
            sandbox_kwargs=supplied_kwargs,
            prepare=prepare,
        )


@dataclass
class BranchSandbox:
    """One E2B sandbox and its matching Chronos branch."""

    adapter: ChronosE2B
    template: str
    workspace: str
    database_envs: dict[str, str]
    from_branch: str
    filesystem: FilesystemAccess
    ttl_seconds: int
    timeout: int
    branch_id: str
    mountpoint: str
    envs: dict[str, str]
    metadata: dict[str, str]
    sandbox_kwargs: dict[str, Any]
    prepare: Callable[[Any], None] | None = field(repr=False)
    sandbox: Any | None = field(default=None, init=False)
    database_urls: dict[str, str] = field(default_factory=dict, init=False)
    _branch_created: bool = field(default=False, init=False, repr=False)
    _branch_closed: bool = field(default=False, init=False, repr=False)
    _mounted: bool = field(default=False, init=False, repr=False)
    _started: bool = field(default=False, init=False, repr=False)
    _cleaned: bool = field(default=False, init=False, repr=False)

    @property
    def _collection_path(self) -> str:
        workspace = quote(self.workspace, safe="")
        return f"/v1/workspaces/{workspace}/branches"

    @property
    def _branch_path(self) -> str:
        return f"{self._collection_path}/{quote(self.branch_id, safe='')}"

    def __enter__(self) -> BranchSandbox:
        return self.start()

    def __exit__(self, exception_type: Any, exception: Any, traceback: Any) -> bool:
        self.close(suppress_errors=exception is not None)
        return False

    def start(self) -> BranchSandbox:
        """Create the Chronos branch, sandbox, and filesystem attachment."""

        if self._started:
            raise ChronosE2BError("this branch-backed sandbox has already been started")
        if self._cleaned:
            raise ChronosE2BError(
                "this branch-backed sandbox has already been cleaned up"
            )
        try:
            branch = self._create_branch()
            self.database_urls = dict(branch.get("database_urls", {}))
            missing = set(self.database_envs).difference(self.database_urls)
            if missing:
                names = ", ".join(sorted(missing))
                raise ChronosE2BError(f"gateway omitted database URLs for: {names}")

            sandbox_env = dict(self.envs)
            for database, variable in self.database_envs.items():
                if variable in sandbox_env:
                    raise ChronosE2BError(
                        f"environment variable {variable} conflicts with database {database}"
                    )
                sandbox_env[variable] = self.database_urls[database]
            sandbox_metadata = dict(self.metadata)
            sandbox_metadata.update(
                {
                    "chronos_workspace": self.workspace,
                    "chronos_branch": self.branch_id,
                }
            )
            self.sandbox = self.adapter._sandbox_type.create(
                self.template,
                timeout=self.timeout,
                envs=sandbox_env,
                metadata=sandbox_metadata,
                **self.sandbox_kwargs,
            )
            if self.prepare is not None:
                self.prepare(self.sandbox)
            if self.filesystem != "none":
                self._attach_filesystem()
            self._started = True
            return self
        except BaseException:
            self.close(suppress_errors=True)
            raise

    def run(self, command: str, **kwargs: Any) -> Any:
        """Run a command through the ordinary E2B command interface."""

        if self.sandbox is None or not self._started:
            raise ChronosE2BError("the sandbox is not running")
        return self.sandbox.commands.run(command, **kwargs)

    def revoke(self) -> None:
        """Immediately revoke SQL and filesystem access without killing E2B."""

        if not self._branch_created or self._branch_closed:
            return
        self.adapter._control.request("DELETE", self._branch_path)
        self._branch_closed = True

    def close(self, *, suppress_errors: bool = False) -> None:
        """Synchronize and remove the sandbox and branch."""

        if self._cleaned:
            return
        errors: list[BaseException] = []
        if self.sandbox is not None and self._mounted:
            if self._branch_closed:
                try:
                    self._run_checked(
                        f"umount -l {shlex.quote(self.mountpoint)}",
                        user="root",
                        timeout=15,
                    )
                    self._mounted = False
                except BaseException as fallback_error:
                    errors.append(fallback_error)
            else:
                try:
                    self._run_checked(
                        f"sync {shlex.quote(self.mountpoint)}",
                        user="root",
                        timeout=30,
                    )
                    self._run_checked(
                        f"umount {shlex.quote(self.mountpoint)}",
                        user="root",
                        timeout=30,
                    )
                    self._mounted = False
                except BaseException as error:
                    errors.append(error)
                    try:
                        self._run_checked(
                            f"umount -l {shlex.quote(self.mountpoint)}",
                            user="root",
                            timeout=15,
                        )
                        self._mounted = False
                    except BaseException as fallback_error:
                        errors.append(fallback_error)
        if self.sandbox is not None:
            try:
                self.sandbox.kill()
            except BaseException as error:
                errors.append(error)
        if self._branch_created and not self._branch_closed:
            try:
                self.revoke()
            except BaseException as error:
                errors.append(error)
        self._cleaned = True
        if errors:
            cleanup_error = ChronosCleanupError(errors)
            if suppress_errors:
                warnings.warn(str(cleanup_error), RuntimeWarning, stacklevel=2)
            else:
                raise cleanup_error

    def _create_branch(self) -> dict[str, Any]:
        body = {
            "branch_id": self.branch_id,
            "from_branch": self.from_branch,
            "databases": list(self.database_envs),
            "filesystem": self.filesystem,
            "ttl_seconds": self.ttl_seconds,
        }
        try:
            branch = self.adapter._control.request("POST", self._collection_path, body)
        except ChronosControlError as create_error:
            if create_error.status is not None:
                raise
            # The POST may have reached the gateway before its response was lost.
            # Reconcile by the caller-generated branch ID instead of retrying it.
            try:
                branch = self.adapter._control.request("GET", self._branch_path)
            except ChronosControlError:
                raise create_error
        self._branch_created = True
        if branch.get("workspace") != self.workspace:
            raise ChronosE2BError("gateway returned a branch in the wrong workspace")
        if branch.get("branch_id") != self.branch_id:
            raise ChronosE2BError("gateway returned the wrong branch")
        return branch

    def _attach_filesystem(self) -> None:
        assert self.sandbox is not None
        address_result = self._run_checked("hostname -I", timeout=30)
        addresses = address_result.stdout.split()
        if not addresses:
            raise ChronosE2BError("E2B sandbox did not report an IP address")
        try:
            address = str(ipaddress.ip_address(addresses[0]))
        except ValueError as error:
            raise ChronosE2BError(
                f"E2B sandbox returned an invalid IP address: {addresses[0]!r}"
            ) from error
        sandbox_id = getattr(self.sandbox, "sandbox_id", "")
        if not sandbox_id:
            raise ChronosE2BError("E2B sandbox did not report a sandbox ID")
        attachment = self.adapter._control.request(
            "POST",
            self._branch_path + "/attach",
            {"sandbox_id": sandbox_id, "sandbox_ip": address},
        )
        try:
            export = attachment["nfs_export"]
            options = attachment["nfs_options"]
        except KeyError as error:
            raise ChronosE2BError(
                f"gateway attach response omitted {error.args[0]}"
            ) from error
        if self.filesystem == "read_only":
            options += ",ro"
        self._run_checked(
            f"mkdir -p -- {shlex.quote(self.mountpoint)}",
            user="root",
            timeout=30,
        )
        self._run_checked(
            'mount -t nfs4 -o "$CHRONOS_NFS_OPTIONS" '
            f'"$CHRONOS_NFS_EXPORT" {shlex.quote(self.mountpoint)}',
            user="root",
            timeout=90,
            envs={
                "CHRONOS_NFS_EXPORT": export,
                "CHRONOS_NFS_OPTIONS": options,
            },
        )
        self._mounted = True
        self._run_checked(
            f"mountpoint -q {shlex.quote(self.mountpoint)}",
            user="root",
            timeout=30,
        )

    def _run_checked(self, command: str, **kwargs: Any) -> Any:
        assert self.sandbox is not None
        result = self.sandbox.commands.run(command, **kwargs)
        if result.exit_code != 0:
            raise ChronosE2BError(
                f"sandbox command failed ({result.exit_code}): {command}\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        return result
