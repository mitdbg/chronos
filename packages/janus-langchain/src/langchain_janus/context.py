"""Janus Context — transactional wrapper for tool calls.

Provides ``JanusContext``, a context manager that orchestrates the full
transactional lifecycle around Janus tools:

  1. **begin()** — mounts an OverlayFS overlay, creates tools bound to it.
  2. Tools operate on the isolated overlay.
  3. **commit()** — merges overlay changes to the real project.
  4. **abort()** — discards all changes.

Also provides ``tar_tool_node()`` which wraps a set of tool calls so
that the model can explicitly begin/commit/abort/savepoint transactions.

Example::

    ctx = JanusContext("/path/to/project")
    ctx.begin()

    # Tools are ready — pass them to the agent
    tools = ctx.get_tools()

    # ... agent uses tools ...

    ctx.commit()   # or ctx.abort()

As a context manager::

    with JanusContext("/path/to/project") as ctx:
        tools = ctx.get_tools()
        # ... agent uses tools ...
    # auto-aborts if not committed
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from langchain_core.callbacks import CallbackManagerForToolRun
from langchain_core.tools import BaseTool
from pydantic import BaseModel, ConfigDict, Field

from janus_core.transaction.coordinator import TransactionCoordinator
from janus_core.transaction.shim_fs import OverlayFSShim
from janus_core.transaction.shim_sqlite import SQLiteShim
from janus_core.transaction.shim_vec import SqliteVecShim
from janus_core.transaction.types import TransactionHandle

from langchain_janus.bash import JanusBash
from langchain_janus.file_editor import JanusFileEditor
from langchain_janus.memory import JanusMemory
from langchain_janus.sqlite_tool import JanusSQLite
from langchain_janus.vectorstore_tool import JanusVectorStore

logger = logging.getLogger(__name__)


class JanusContext:
    """Transactional context wrapping OverlayFS + coordinator + tools.

    Manages the lifecycle of a single transaction: begin sets up the
    overlay mount, tools operate on it, and commit/abort finalizes.

    Supports subtransactions (savepoints): ``savepoint(name)`` and
    ``rollback(name)`` delegate to the coordinator's child transaction
    mechanism.

    Args:
        base_path: Path to the real project directory.
        coordinator: Optional pre-configured TransactionCoordinator.
            If None, one is created and the OverlayFSShim is auto-registered.
        db_path: SQLite database path. Defaults to ":memory:".
            Pass a file path for persistent storage across sessions.
        enable_sqlite: Whether to enable the SQLite tool (default True).
        enable_vectorstore: Whether to enable the VectorStore tool (default True).
        vector_dimensions: Default embedding dimensions for vector store.
        weak_snapshot: If True, keep snapshot reads but disable eager write
            conflict detection (SQLite write locks and OverlayFS commit-time
            conflict checks). This provides weaker isolation semantics.
    """

    def __init__(
        self,
        base_path: str | Path,
        coordinator: TransactionCoordinator | None = None,
        db_path: str = ":memory:",
        enable_sqlite: bool = True,
        enable_vectorstore: bool = True,
        vector_dimensions: int = 384,
        weak_snapshot: bool = False,
    ) -> None:
        self._base_path = Path(base_path).resolve()
        self._weak_snapshot = bool(weak_snapshot)
        self._shim = OverlayFSShim(
            self._base_path,
            enable_conflict_detection=not self._weak_snapshot,
        )
        self._enable_sqlite = enable_sqlite
        self._enable_vectorstore = enable_vectorstore
        self._vector_dimensions = vector_dimensions

        # SQLite shim (participates in same 2PC)
        self._sqlite_shim: SQLiteShim | None = None
        if enable_sqlite:
            self._sqlite_shim = SQLiteShim(
                db_path,
                enforce_write_locks=not self._weak_snapshot,
                enforce_snapshot_validation=not self._weak_snapshot,
            )

        # Vector store shim (participates in same 2PC)
        self._vec_shim: SqliteVecShim | None = None
        if enable_vectorstore:
            self._vec_shim = SqliteVecShim(db_path, dimensions=vector_dimensions)

        if coordinator is not None:
            self._coordinator = coordinator
        else:
            self._coordinator = TransactionCoordinator()
            self._coordinator.register_shim(self._shim)
            if self._sqlite_shim is not None:
                self._coordinator.register_shim(self._sqlite_shim)
            if self._vec_shim is not None:
                self._coordinator.register_shim(self._vec_shim)

        self._txn: TransactionHandle | None = None
        self._child_txn: TransactionHandle | None = None
        self._tools_created = False

        # Tool instances — created on begin()
        self._file_editor: JanusFileEditor | None = None
        self._memory: JanusMemory | None = None
        self._bash: JanusBash | None = None
        self._sqlite: JanusSQLite | None = None
        self._vectorstore: JanusVectorStore | None = None

    # ── Properties ───────────────────────────────────────────────────

    @property
    def coordinator(self) -> TransactionCoordinator:
        """The transaction coordinator."""
        return self._coordinator

    @property
    def shim(self) -> OverlayFSShim:
        """The OverlayFS shim."""
        return self._shim

    @property
    def sqlite_shim(self) -> SQLiteShim | None:
        """The SQLite shim (None if disabled)."""
        return self._sqlite_shim

    @property
    def vec_shim(self) -> SqliteVecShim | None:
        """The vector store shim (None if disabled)."""
        return self._vec_shim

    @property
    def weak_snapshot(self) -> bool:
        """Whether weak snapshot mode is enabled."""
        return self._weak_snapshot

    @property
    def txn(self) -> TransactionHandle | None:
        """The current transaction handle (None if not begun)."""
        return self._txn

    @property
    def is_active(self) -> bool:
        """Whether a transaction is currently active."""
        return self._txn is not None and self._txn.is_active

    @property
    def working_dir(self) -> Path | None:
        """The currently active overlay merged directory (None if not begun).

        Returns the child (savepoint) overlay if one is active, otherwise
        returns the parent transaction's overlay. This ensures that direct
        filesystem writes go to the correct isolated layer.
        """
        if self._txn is None:
            return None
        # Prefer the active child overlay (savepoint) if one exists
        if self._child_txn is not None and self._child_txn.is_active:
            return self._shim.get_working_directory(self._child_txn)
        return self._shim.get_working_directory(self._txn)

    @property
    def file_editor(self) -> JanusFileEditor:
        """File editor tool bound to the current transaction."""
        if self._file_editor is None:
            raise RuntimeError("Transaction not begun. Call begin() first.")
        return self._file_editor

    @property
    def memory(self) -> JanusMemory:
        """Memory tool bound to the current transaction."""
        if self._memory is None:
            raise RuntimeError("Transaction not begun. Call begin() first.")
        return self._memory

    @property
    def bash(self) -> JanusBash:
        """Bash tool bound to the current transaction."""
        if self._bash is None:
            raise RuntimeError("Transaction not begun. Call begin() first.")
        return self._bash

    @property
    def sqlite(self) -> JanusSQLite:
        """SQLite tool bound to the current transaction."""
        if self._sqlite is None:
            raise RuntimeError(
                "SQLite not available. Either transaction not begun "
                "or enable_sqlite=False."
            )
        return self._sqlite

    @property
    def vectorstore(self) -> JanusVectorStore:
        """VectorStore tool bound to the current transaction."""
        if self._vectorstore is None:
            raise RuntimeError(
                "VectorStore not available. Either transaction not begun "
                "or enable_vectorstore=False."
            )
        return self._vectorstore

    # ── Lifecycle ────────────────────────────────────────────────────

    def begin(self) -> TransactionHandle:
        """Begin a new transaction, mounting the overlay.

        Returns:
            The new TransactionHandle.

        Raises:
            RuntimeError: If a transaction is already active.
        """
        if self._txn is not None and self._txn.is_active:
            raise RuntimeError("Transaction already active. Commit or abort first.")

        self._txn = self._coordinator.begin()
        workdir = self._shim.get_working_directory(self._txn)

        # Create tool instances bound to the overlay
        self._file_editor = JanusFileEditor(working_dir=workdir)
        self._memory = JanusMemory(working_dir=workdir)
        self._bash = JanusBash(working_dir=workdir)
        if self._sqlite_shim is not None:
            self._sqlite = JanusSQLite(shim=self._sqlite_shim, txn=self._txn)
        if self._vec_shim is not None:
            self._vectorstore = JanusVectorStore(shim=self._vec_shim, txn=self._txn)
        self._tools_created = True

        logger.info("Janus transaction begun (txn=%s, workdir=%s)", self._txn.id, workdir)
        return self._txn

    def commit(self) -> None:
        """Commit the current transaction, merging changes to the real project.

        Raises:
            RuntimeError: If no transaction is active.
        """
        if self._txn is None or not self._txn.is_active:
            raise RuntimeError("No active transaction to commit.")

        # Commit any active child first so its work is included
        if self._child_txn is not None and self._child_txn.is_active:
            self._coordinator.commit_child(self._child_txn.id)
            self._child_txn = None
            self._restore_tools_to_parent()

        self._coordinator.commit(self._txn.id)  # returns list[ChangeRecord]
        logger.info("Janus transaction committed (txn=%s)", self._txn.id)
        self._cleanup_tools()

    def abort(self) -> None:
        """Abort the current transaction, discarding all changes.

        Raises:
            RuntimeError: If no transaction is active.
        """
        if self._txn is None or not self._txn.is_active:
            raise RuntimeError("No active transaction to abort.")

        # Abort any active child first
        if self._child_txn is not None and self._child_txn.is_active:
            self._coordinator.abort_child(self._child_txn.id)
            self._child_txn = None
            self._restore_tools_to_parent()

        self._coordinator.rollback(self._txn.id)
        logger.info("Janus transaction aborted (txn=%s)", self._txn.id)
        self._cleanup_tools()

    def savepoint(self, name: str) -> None:
        """Create a savepoint (implemented as a child subtransaction).

        All tools are switched to the child overlay so that writes
        made after this point can be rolled back independently.

        Args:
            name: Name for the savepoint.
        """
        if self._txn is None or not self._txn.is_active:
            raise RuntimeError("No active transaction for savepoint.")

        # If there's an existing child, commit it first (sequential savepoints)
        if self._child_txn is not None and self._child_txn.is_active:
            self._coordinator.commit_child(self._child_txn.id)
            self._child_txn = None
            self._restore_tools_to_parent()

        self._child_txn = self._coordinator.begin_child(self._txn.id)
        self._switch_tools_to_child()
        logger.info(
            "Janus savepoint '%s' created (child=%s)", name, self._child_txn.id
        )

    def rollback(self, name: str | None = None) -> None:
        """Rollback to the last savepoint (abort the child subtransaction).

        Discards the child overlay (FS) and child MVCC writes (DB),
        then restores all tools to the parent transaction's state.

        Args:
            name: Optional savepoint name (for logging only in this impl).
        """
        if self._child_txn is None or not self._child_txn.is_active:
            raise RuntimeError("No active savepoint to rollback.")

        self._coordinator.abort_child(self._child_txn.id)
        logger.info(
            "Janus savepoint rolled back (child=%s, name=%s)",
            self._child_txn.id,
            name,
        )
        self._child_txn = None
        self._restore_tools_to_parent()

    def get_changes(self) -> list[Any]:
        """Get all changes made in the current transaction.

        Returns:
            List of ChangeRecord objects from all enrolled shims.
        """
        if self._txn is None:
            return []
        changes = list(self._shim.get_changes(self._txn))
        if self._sqlite_shim is not None:
            changes.extend(self._sqlite_shim.get_changes(self._txn))
        if self._vec_shim is not None:
            changes.extend(self._vec_shim.get_changes(self._txn))
        return changes

    def get_tools(self) -> list[BaseTool]:
        """Return all Janus tools bound to the current transaction.

        Returns:
            List of tools: [JanusFileEditor, JanusMemory, JanusBash] plus
            JanusSQLite and JanusVectorStore if enabled.

        Raises:
            RuntimeError: If transaction not begun.
        """
        if not self._tools_created:
            raise RuntimeError("Transaction not begun. Call begin() first.")
        tools: list[BaseTool] = [self.file_editor, self.memory, self.bash]
        if self._sqlite is not None:
            tools.append(self._sqlite)
        if self._vectorstore is not None:
            tools.append(self._vectorstore)
        return tools

    # ── Context manager ──────────────────────────────────────────────

    def __enter__(self) -> JanusContext:
        self.begin()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._txn is not None and self._txn.is_active:
            self.abort()

    # ── Private ──────────────────────────────────────────────────────

    def _switch_tools_to_child(self) -> None:
        """Update all tools to use the child subtransaction's overlay.

        For FS tools (file_editor, memory, bash): switches working_dir
        to the child overlay's merged directory.
        For SQLite: switches txn handle to the child transaction so
        that MVCC writes use the child's numeric_id.
        """
        assert self._child_txn is not None
        child_workdir = self._shim.get_working_directory(self._child_txn)
        if self._file_editor is not None:
            self._file_editor.working_dir = child_workdir
        if self._memory is not None:
            self._memory.working_dir = child_workdir
        if self._bash is not None:
            self._bash.working_dir = child_workdir
        if self._sqlite is not None:
            self._sqlite.txn = self._child_txn
        if self._vectorstore is not None:
            self._vectorstore.txn = self._child_txn

    def _restore_tools_to_parent(self) -> None:
        """Restore all tools to the parent transaction's overlay.

        Called after child abort/commit to return tools to the parent
        working directory and transaction handle.
        """
        assert self._txn is not None
        parent_workdir = self._shim.get_working_directory(self._txn)
        if self._file_editor is not None:
            self._file_editor.working_dir = parent_workdir
        if self._memory is not None:
            self._memory.working_dir = parent_workdir
        if self._bash is not None:
            self._bash.working_dir = parent_workdir
        if self._sqlite is not None:
            self._sqlite.txn = self._txn
        if self._vectorstore is not None:
            self._vectorstore.txn = self._txn

    def _commit_active_savepoint(self) -> None:
        """Commit the current child subtransaction (savepoint) into parent.

        Called by TxnContext when a subtransaction commits explicitly.
        Makes the child's filesystem and DB changes visible in the parent.
        """
        if self._child_txn is not None and self._child_txn.is_active:
            self._coordinator.commit_child(self._child_txn.id)
            self._child_txn = None
            self._restore_tools_to_parent()
            logger.info("Janus active savepoint committed into parent txn")

    def _cleanup_tools(self) -> None:
        """Clear tool references after commit/abort."""
        self._file_editor = None
        self._memory = None
        self._bash = None
        self._sqlite = None
        self._vectorstore = None
        self._tools_created = False


# ── Janus Transaction Control Tool ─────────────────────────────────────


class JanusTransactionControlInput(BaseModel):
    """Input schema for transactional control primitives."""

    action: str = Field(
        description=(
            "Transaction control action. One of: "
            "'begin' - Start a new transaction. "
            "'commit' - Commit the current transaction (merge to real project). "
            "'abort' - Abort the current transaction (discard all changes). "
            "'savepoint' - Create a savepoint for later rollback. "
            "'rollback' - Rollback to the last savepoint. "
            "'status' - Check current transaction status. "
            "'changes' - List all changes made in the current transaction."
        )
    )
    name: str | None = Field(
        default=None,
        description="Savepoint name (required for 'savepoint', optional for 'rollback').",
    )


class JanusTransactionControl(BaseTool):
    """Tool that exposes Janus transaction control to the model.

    Allows the model to explicitly manage the transaction lifecycle:
    begin, commit, abort, savepoint, and rollback. This lets the model
    learn to use transactional primitives for safe exploration.

    Example::

        ctx = JanusContext("/path/to/project")
        txn_ctl = JanusTransactionControl(janus_context=ctx)

        # Model can now call:
        txn_ctl.invoke({"action": "begin"})
        txn_ctl.invoke({"action": "savepoint", "name": "before_refactor"})
        txn_ctl.invoke({"action": "rollback"})
        txn_ctl.invoke({"action": "commit"})
    """

    name: str = "janus_txn"
    description: str = (
        "Control the transactional lifecycle. Actions: "
        "begin, commit, abort, savepoint, rollback, status, changes. "
        "Use 'begin' before making changes, 'savepoint' before risky "
        "operations, 'rollback' to undo, and 'commit' when satisfied."
    )
    args_schema: type[BaseModel] = JanusTransactionControlInput

    janus_context: JanusContext
    """The JanusContext to control."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _run(
        self,
        action: str,
        name: str | None = None,
        run_manager: CallbackManagerForToolRun | None = None,
    ) -> str:
        """Execute a transaction control action."""
        try:
            if action == "begin":
                txn = self.janus_context.begin()
                return (
                    f"Transaction started (id={txn.id}). "
                    f"All file changes are now isolated. "
                    f"Use 'commit' to apply or 'abort' to discard."
                )

            elif action == "commit":
                self.janus_context.commit()
                return (
                    "Transaction committed. All changes have been merged "
                    "to the real project directory."
                )

            elif action == "abort":
                self.janus_context.abort()
                return (
                    "Transaction aborted. All changes have been discarded."
                )

            elif action == "savepoint":
                if name is None:
                    return "Error: 'name' is required for savepoint."
                self.janus_context.savepoint(name)
                return (
                    f"Savepoint '{name}' created. "
                    f"Use 'rollback' to return to this point."
                )

            elif action == "rollback":
                self.janus_context.rollback(name)
                return (
                    f"Rolled back to savepoint"
                    + (f" '{name}'" if name else "")
                    + ". Changes since the savepoint have been discarded."
                )

            elif action == "status":
                return self._get_status()

            elif action == "changes":
                return self._get_changes()

            else:
                return f"Error: Unknown action '{action}'."

        except Exception as e:
            return f"Error: {e}"

    def _get_status(self) -> str:
        """Get current transaction status."""
        if not self.janus_context.is_active:
            return "No active transaction."

        txn = self.janus_context.txn
        assert txn is not None
        workdir = self.janus_context.working_dir
        changes = self.janus_context.get_changes()

        return (
            f"Transaction active (id={txn.id})\n"
            f"  Working directory: {workdir}\n"
            f"  Changes: {len(changes)} file(s) modified\n"
            f"  State: {txn.state.value}"
        )

    def _get_changes(self) -> str:
        """List all changes in the current transaction."""
        if not self.janus_context.is_active:
            return "No active transaction."

        changes = self.janus_context.get_changes()
        if not changes:
            return "No changes made yet."

        lines = ["Changes in current transaction:"]
        for ch in changes:
            lines.append(f"  [{ch.change_type.value}] {ch.resource_id}")
        return "\n".join(lines)


def create_janus_tools(
    base_path: str | Path,
    *,
    weak_snapshot: bool = False,
) -> tuple[JanusContext, list[BaseTool]]:
    """Convenience: create a JanusContext and return all tools including txn control.

    Returns:
        Tuple of (context, tools) where tools includes file_editor, memory,
        bash, and txn_control. The context must be managed (begin/commit/abort)
        either manually or via the txn_control tool.

    Note:
        The context is NOT begun automatically. Either call ``ctx.begin()``
        or let the model use the ``janus_txn`` tool with action='begin'.
    """
    ctx = JanusContext(base_path, weak_snapshot=weak_snapshot)
    txn_ctl = JanusTransactionControl(janus_context=ctx)

    # We can't return the other tools yet because they aren't created
    # until begin() is called. Return the control tool and let the model
    # or the caller begin the transaction.
    return ctx, [txn_ctl]


def create_janus_tools_with_active_txn(
    base_path: str | Path,
    *,
    weak_snapshot: bool = False,
) -> tuple[JanusContext, list[BaseTool]]:
    """Create a JanusContext, begin a transaction, and return all tools.

    This is the most common pattern: create everything ready-to-use.

    Returns:
        Tuple of (context, tools) where context has an active transaction
        and tools includes file_editor, memory, bash, and txn_control.

    Note:
        Caller is responsible for calling ``ctx.commit()`` or ``ctx.abort()``
        when done. Using ``ctx`` as a context manager also works.
    """
    ctx = JanusContext(base_path, weak_snapshot=weak_snapshot)
    ctx.begin()
    txn_ctl = JanusTransactionControl(janus_context=ctx)
    tools = ctx.get_tools() + [txn_ctl]
    return ctx, tools
