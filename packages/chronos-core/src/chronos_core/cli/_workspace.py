"""Workspace lifecycle commands and invocation-owned connections and sessions."""
from __future__ import annotations

import sqlite3
import sys
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

import psycopg
import typer

from chronos_core.cli._config import (
    DatabaseConfig, QdrantConfig, WorkspaceConfig, WorkspaceConfigError, load_workspace_config, open_workspace,
)
from chronos_core.cli._terminal import MergePolicy, json_object_option, print_result, report_error

if TYPE_CHECKING:
    from chronos_core.workspace import ChronosWorkspaceContext
    from chronos_core.workspace.runtime import WorkspaceBranchSession


@dataclass(frozen=True)
class WorkspaceOptions:
    config_path: Path | None


branch_app = typer.Typer(no_args_is_help=True)
checkpoint_app = typer.Typer(no_args_is_help=True)
merge_app = typer.Typer(no_args_is_help=True)


@dataclass(frozen=True)
class _ErrorPolicy:
    """Expected operational errors and the configured values to redact from them."""
    errors: tuple[type[BaseException], ...]
    secrets: tuple[str | None, ...]
    urls: tuple[str | None, ...]

    def report(self, error: Exception) -> NoReturn:
        report_error(error, secrets=self.secrets, urls=self.urls)


@dataclass(frozen=True)
class _HeldWorkspace:
    config: WorkspaceConfig
    policy: _ErrorPolicy
    workspace: ChronosWorkspaceContext


# Set only while persistent mode keeps one workspace open for many commands.
_held: _HeldWorkspace | None = None


def held_workspace() -> _HeldWorkspace | None:
    return _held


@dataclass
class _OpenTransaction:
    """A branch session whose transaction() stays entered between persistent-mode lines."""
    store: str
    branch: str
    workspace: ChronosWorkspaceContext
    checked_out: WorkspaceBranchSession
    scope: ExitStack
    failed: bool = False


class _Rollback(Exception):
    """Passed into the held transaction context so the API rolls it back."""


# Commands allowed while a transaction is open; only rollback once it has failed.
_IN_TRANSACTION = {("query",), ("execute",), ("upsert",), ("commit",), ("rollback",)}
_transaction: _OpenTransaction | None = None


def _refuse(message: str) -> NoReturn:
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(1)


def _check_open_transaction(ctx: typer.Context) -> None:
    if _transaction is None:
        return
    command = tuple(ctx.command_path.split()[1:])
    if _transaction.failed and command != ("rollback",):
        _refuse("a command failed in the open transaction; only rollback is allowed.")
    if command == ("begin",):
        _refuse("a transaction is already open; commit or roll back first.")
    if command not in _IN_TRANSACTION:
        _refuse(
            f"a transaction is open on store {_transaction.store!r}, branch {_transaction.branch!r}; "
            "commit or roll back first."
        )


def _release_sessions(workspace: ChronosWorkspaceContext, checked_out: WorkspaceBranchSession) -> list[Exception]:
    # SQL sessions expose close(). Qdrant control sessions without a public
    # close method are released by their owning context when it closes.
    errors: list[Exception] = []
    for session in checked_out.stores.values():
        close = getattr(session, "close", None)
        if callable(close):
            try:
                close()
            except Exception as error:
                errors.append(error)
    # The OverlayFS store keeps a checked-out branch mounted until unmount() or
    # close(). Release it per command so persistent mode keeps the one-shot
    # guarantee that each invocation releases its mounts.
    unmount = getattr(workspace.filesystem, "unmount", None)
    if checked_out.fs is not None and callable(unmount):
        try:
            unmount(checked_out.branch_id)
        except Exception as error:
            errors.append(error)
    return errors


def begin_transaction(workspace: ChronosWorkspaceContext, store: str, branch: str) -> None:
    """Enter the store session's transaction() and keep it open for later lines."""
    global _transaction
    if _transaction is not None:
        _refuse("a transaction is already open; commit or roll back first.")
    checked_out = workspace.checkout(branch)
    scope = ExitStack()
    try:
        scope.enter_context(checked_out.stores[store].transaction())
    except BaseException:
        _release_sessions(workspace, checked_out)
        raise
    _transaction = _OpenTransaction(store, branch, workspace, checked_out, scope)


def end_transaction(*, commit: bool) -> None:
    """Leave the held transaction() normally (commit) or with an exception (rollback)."""
    global _transaction
    transaction = _transaction
    if transaction is None:
        _refuse("no transaction is open.")
    _transaction = None
    try:
        if commit:
            transaction.scope.close()
        else:
            transaction.scope.__exit__(_Rollback, _Rollback(), None)
    finally:
        errors = _release_sessions(transaction.workspace, transaction.checked_out)
    if errors:
        raise errors[0]


def _load_config(ctx: typer.Context, store: str | None, qdrant: bool) -> WorkspaceConfig:
    if _held is not None:
        config = _held.config
    else:
        options = ctx.find_object(WorkspaceOptions)
        try:
            config = load_workspace_config(options.config_path if options else None)
        except WorkspaceConfigError as error:
            raise typer.BadParameter(str(error), param_hint="--config") from error
    if store is not None:
        try:
            config.store(store, qdrant=qdrant)
        except WorkspaceConfigError as error:
            raise typer.BadParameter(str(error), param_hint="--store") from error
    return config


def _error_policy(config: WorkspaceConfig) -> _ErrorPolicy:
    from chronos_core.branching import BranchingError
    from chronos_core.workspace import ChronosFSError, ChronosFSMountError, FilesystemStoreError

    errors: tuple[type[BaseException], ...] = (
        ChronosFSError, ChronosFSMountError, FilesystemStoreError, BranchingError,
        RuntimeError, ValueError, TypeError, OSError, sqlite3.Error, psycopg.Error,
    )
    secrets: list[str | None] = []
    urls: list[str | None] = []
    if isinstance(config.filesystem, DatabaseConfig):
        urls.extend((config.filesystem.database_url, config.filesystem.metadata_url))
    for entry in config.stores.values():
        target = entry.connection
        urls.append(target.metadata_url)
        if isinstance(target, QdrantConfig):
            secrets.append(target.api_key)
            urls.append(target.url)
        else:
            urls.append(target.database_url)
    if any(entry.kind == "qdrant" for entry in config.stores.values()):
        try:
            import grpc
            import httpx
            from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse
        except ImportError:
            report_error(RuntimeError("Qdrant support requires installing chronos-core[qdrant]."))
        errors += (grpc.RpcError, httpx.HTTPError, ResponseHandlingException, UnexpectedResponse)
    return _ErrorPolicy(errors, tuple(secrets), tuple(urls))


@contextmanager
def connect_workspace(
    ctx: typer.Context, store: str | None = None, *, qdrant: bool = False,
) -> Generator[ChronosWorkspaceContext, None, None]:
    config = _load_config(ctx, store, qdrant)
    if _held is not None:
        # Persistent mode: reuse the open workspace and leave closing to its owner.
        _check_open_transaction(ctx)
        try:
            yield _held.workspace
        except typer.Exit:
            raise  # an exit the command chose; Exit subclasses RuntimeError
        except _held.policy.errors as error:
            _held.policy.report(error)
        return
    policy = _error_policy(config)
    try:
        with open_workspace(config) as workspace:
            yield workspace
    except typer.Exit:
        raise  # an exit the command chose; Exit subclasses RuntimeError
    except policy.errors as error:
        policy.report(error)


@contextmanager
def hold_workspace(ctx: typer.Context) -> Generator[_HeldWorkspace, None, None]:
    """Open the configured workspace once for persistent mode."""
    global _held
    config = _load_config(ctx, None, False)
    policy = _error_policy(config)
    try:
        with open_workspace(config) as workspace:
            _held = _HeldWorkspace(config, policy, workspace)
            try:
                yield _held
            finally:
                if _transaction is not None:
                    # Input ended with a transaction open: roll it back, never commit.
                    try:
                        end_transaction(commit=False)
                    except Exception as error:
                        try:
                            policy.report(error)
                        except typer.Exit:
                            pass
                _held = None
    except policy.errors as error:
        policy.report(error)


@contextmanager
def workspace_session(workspace: ChronosWorkspaceContext, branch: str) -> Generator[WorkspaceBranchSession, None, None]:
    checked_out = workspace.checkout(branch)
    try:
        yield checked_out
    finally:
        errors = _release_sessions(workspace, checked_out)
        if errors and sys.exc_info()[0] is None:
            raise errors[0]


@contextmanager
def store_session(workspace: ChronosWorkspaceContext, branch: str, store: str) -> Generator[Any, None, None]:
    transaction = _transaction
    if transaction is None:
        with workspace_session(workspace, branch) as checked_out:
            yield checked_out.stores[store]
        return
    if (store, branch) != (transaction.store, transaction.branch):
        _refuse(
            f"the open transaction is on store {transaction.store!r}, branch {transaction.branch!r}; "
            "commit or roll back first."
        )
    try:
        yield transaction.checked_out.stores[store]
    except BaseException:
        transaction.failed = True
        raise


@branch_app.command("list")
def list_branches(ctx: typer.Context, store: str | None = typer.Option(
    None, "--store", help="List one relational store's branch records, including metadata.",
)) -> None:
    with connect_workspace(ctx, store) as workspace:
        print_result(workspace.list_branches() if store is None else workspace.stores[store].list_branches())


@branch_app.command("create")
def create_branch(ctx: typer.Context, name: str, from_branch: str = typer.Option("main", "--from"),
                  metadata: str | None = typer.Option(None, "--metadata")) -> None:
    values = json_object_option(metadata, "--metadata")
    with connect_workspace(ctx) as workspace:
        print_result(workspace.create_branch(name, from_branch=from_branch, metadata=values))


@branch_app.command("update-metadata")
def update_branch_metadata(ctx: typer.Context, name: str, metadata: str,
                           store: str = typer.Option(..., "--store")) -> None:
    values = json_object_option(metadata, "METADATA")
    with connect_workspace(ctx, store) as workspace:
        print_result(workspace.stores[store].update_branch_metadata(name, values))


@branch_app.command("create-from-checkpoint")
def create_branch_from_checkpoint(ctx: typer.Context, name: str, checkpoint: str) -> None:
    with connect_workspace(ctx) as workspace:
        print_result(workspace.create_branch_from_checkpoint(name, checkpoint))


@branch_app.command("delete")
def delete_branch(ctx: typer.Context, name: str) -> None:
    with connect_workspace(ctx) as workspace:
        print_result(workspace.delete_branch(name))


@checkpoint_app.command("create")
def create_checkpoint(ctx: typer.Context, name: str, branch: str = typer.Option("main", "--branch"),
                      metadata: str | None = typer.Option(None, "--metadata")) -> None:
    values = json_object_option(metadata, "--metadata")
    with connect_workspace(ctx) as workspace:
        print_result(workspace.create_checkpoint(name, branch=branch, metadata=values))


@checkpoint_app.command("list")
def list_checkpoints(ctx: typer.Context, store: str = typer.Option(..., "--store"),
                     branch: str | None = typer.Option(None, "--branch"),
                     metadata_filter: str | None = typer.Option(None, "--metadata-filter")) -> None:
    values = json_object_option(metadata_filter, "--metadata-filter")
    with connect_workspace(ctx, store) as workspace:
        print_result(workspace.stores[store].list_checkpoints(branch, values))


def diff(ctx: typer.Context, left: str, right: str) -> None:
    with connect_workspace(ctx) as workspace:
        print_result(workspace.diff(left, right))


@merge_app.command("preview")
def merge_preview(ctx: typer.Context, source: str, target: str,
                  policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy")) -> None:
    with connect_workspace(ctx) as workspace:
        print_result(workspace.merge_preview(source, target, policy=policy.api_value))


@merge_app.command("apply")
def merge_apply(ctx: typer.Context, source: str, target: str,
                policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy")) -> None:
    with connect_workspace(ctx) as workspace:
        print_result(workspace.merge_apply(source, target, policy=policy.api_value))


@merge_app.command("atomic-preview")
def merge_atomic_preview(ctx: typer.Context, source: str, target: str,
                         policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy")) -> None:
    with connect_workspace(ctx) as workspace:
        print_result(workspace.merge_atomic_preview(source, target, policy=policy.api_value))


@merge_app.command("atomic-apply")
def merge_atomic(
    ctx: typer.Context, source: str, target: str,
    operation_id: str = typer.Option(..., "--operation-id"),
    preview_token: str | None = typer.Option(None, "--preview-token"),
    policy: MergePolicy = typer.Option(MergePolicy.abort_on_conflict, "--policy"),
) -> None:
    with connect_workspace(ctx) as workspace:
        print_result(workspace.merge_atomic(
            source, target, operation_id=operation_id, preview_token=preview_token, policy=policy.api_value,
        ))


def register_commands(app: typer.Typer) -> None:
    app.command()(diff)
    app.add_typer(branch_app, name="branch", help="Manage branches across the workspace.")
    app.add_typer(checkpoint_app, name="checkpoint", help="Checkpoint all participants and list checkpoints.")
    app.add_typer(merge_app, name="merge", help="Preview and apply coordinated or atomic merges.")
