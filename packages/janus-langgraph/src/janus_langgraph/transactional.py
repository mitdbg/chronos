"""Transactional tools, wrappers, and agent factory for LangGraph.

Provides:
- ``create_transaction_tools()`` — LangChain tools that let an agent
  manage transactions (begin, commit, rollback, savepoint, etc.)
- ``TransactionalToolWrapper`` — ``ToolCallWrapper`` that auto-creates
  a savepoint before each tool call and rolls back on failure
- ``create_transactional_agent()`` — convenience factory wiring a
  ``create_react_agent`` with full transaction support
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Sequence

from langchain_core.messages import ToolMessage
from langchain_core.tools import BaseTool, tool

from langgraph.prebuilt import ToolNode, create_react_agent
from langgraph.types import Command
from janus_core.transaction.coordinator import (
    TransactionCoordinator,
    TransactionNotActiveError,
)
from janus_core.transaction.types import TransactionPolicy

logger = logging.getLogger(__name__)


# ── Transaction tools ────────────────────────────────────────────────


def create_transaction_tools(
    coordinator: TransactionCoordinator,
) -> list[BaseTool]:
    """Create LangChain tools for transaction management.

    Returns tools: ``txn_begin``, ``txn_commit``, ``txn_rollback``,
    ``txn_savepoint``, ``txn_rollback_to_savepoint``, ``txn_status``,
    ``txn_list_changes``.
    """

    @tool
    def txn_begin() -> str:
        """Begin a new transaction. All subsequent tool calls will operate
        on a virtual branch. Changes are isolated until committed."""
        try:
            txn = coordinator.begin()
            return json.dumps(
                {
                    "status": "ok",
                    "transaction_id": txn.id,
                    "branch": str(txn.branch_id),
                    "message": "Transaction started. Changes are isolated until you commit.",
                }
            )
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})

    @tool
    def txn_commit() -> str:
        """Commit the current transaction. All branch changes are atomically
        merged to the main state. If conflicts are detected, the commit is
        aborted and an error is returned."""
        try:
            changes = coordinator.commit()
            summaries = [c.summary() for c in changes]
            return json.dumps(
                {
                    "status": "ok",
                    "message": f"Committed {len(changes)} changes.",
                    "changes": summaries,
                }
            )
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})

    @tool
    def txn_rollback(target: str | None = None) -> str:
        """Rollback changes. If `target` is given, rollback to that savepoint.
        Otherwise rollback the entire transaction."""
        try:
            if target:
                coordinator.rollback_to_savepoint(target)
                return json.dumps(
                    {
                        "status": "ok",
                        "message": f"Rolled back to savepoint '{target}'.",
                    }
                )
            else:
                coordinator.rollback()
                return json.dumps(
                    {
                        "status": "ok",
                        "message": "Transaction rolled back.",
                    }
                )
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})

    @tool
    def txn_savepoint(name: str) -> str:
        """Create a named savepoint in the current transaction. You can later
        rollback to this point without losing all transaction progress."""
        try:
            sp = coordinator.savepoint(name)
            return json.dumps(
                {
                    "status": "ok",
                    "savepoint": sp.name,
                    "message": f"Savepoint '{sp.name}' created.",
                }
            )
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})

    @tool
    def txn_status() -> str:
        """Get the current transaction status including active branch,
        savepoints, and pending change count."""
        try:
            info = coordinator.status()
            return json.dumps(info, default=str)
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})

    @tool
    def txn_list_changes() -> str:
        """List all pending changes in the current transaction."""
        try:
            changes = coordinator.get_changes()
            return json.dumps(
                {
                    "changes": [c.summary() for c in changes],
                    "count": len(changes),
                }
            )
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})

    return [
        txn_begin,
        txn_commit,
        txn_rollback,
        txn_savepoint,
        txn_status,
        txn_list_changes,
    ]


# ── Transactional tool wrapper ──────────────────────────────────────


class TransactionalToolWrapper:
    """ToolCallWrapper that provides automatic savepoint/rollback.

    Before each tool call, creates a savepoint. If the tool raises an
    exception, automatically rolls back to the savepoint.

    Usage with ToolNode::

        wrapper = TransactionalToolWrapper(coordinator)
        tool_node = ToolNode(tools, wrap_tool_call=wrapper)
    """

    def __init__(
        self,
        coordinator: TransactionCoordinator,
        *,
        auto_savepoint: bool = True,
        skip_tool_names: set[str] | None = None,
    ):
        self.coordinator = coordinator
        self.auto_savepoint = auto_savepoint
        # Don't wrap transaction management tools themselves
        self._skip = skip_tool_names or {
            "txn_begin",
            "txn_commit",
            "txn_rollback",
            "txn_savepoint",
            "txn_status",
            "txn_list_changes",
        }

    def __call__(
        self,
        request: Any,
        execute: Callable[[Any], ToolMessage | Command],
    ) -> ToolMessage | Command:
        """Synchronous wrapper with auto-savepoint/rollback."""
        tool_name = request.tool_call.get("name", "unknown")  # type: ignore[union-attr]

        # Skip wrapping for transaction-management tools
        if tool_name in self._skip:
            return execute(request)

        # Check if there's an active transaction
        try:
            active = self.coordinator.get_active_transaction()
        except Exception:
            active = None

        if active is None or not active.is_active:
            return execute(request)

        sp_name = f"auto_{tool_name}_{request.tool_call.get('id', 'x')[:6]}"  # type: ignore[union-attr]

        if self.auto_savepoint:
            try:
                self.coordinator.savepoint(sp_name)
            except Exception as e:
                logger.warning("Failed to create auto-savepoint: %s", e)

        try:
            result = execute(request)
            return result
        except Exception as e:
            # Auto-rollback to savepoint
            if self.auto_savepoint:
                try:
                    self.coordinator.rollback_to_savepoint(sp_name)
                    logger.info(
                        "Auto-rolled back to savepoint '%s' after error in tool '%s'",
                        sp_name,
                        tool_name,
                    )
                except Exception as rb_err:
                    logger.error(
                        "Failed to auto-rollback to savepoint '%s': %s",
                        sp_name,
                        rb_err,
                    )
            raise


# ── Agent factory ────────────────────────────────────────────────────


def create_transactional_agent(
    model: Any,
    tools: Sequence[BaseTool | Callable] | ToolNode,
    *,
    coordinator: TransactionCoordinator,
    policy: TransactionPolicy | None = None,
    auto_savepoint: bool = True,
    include_transaction_tools: bool = True,
    **react_agent_kwargs: Any,
) -> Any:
    """Create a react agent with full transaction support.

    Wraps ``create_react_agent`` with:
    - Transaction management tools (begin/commit/rollback/savepoint/status)
    - Automatic savepoint before each tool call
    - Automatic rollback on tool failure

    Args:
        model: LLM to use.
        tools: User tools for the agent.
        coordinator: TransactionCoordinator with shims already registered.
        policy: Optional TransactionPolicy for automatic begin/commit.
        auto_savepoint: Auto-create savepoints before each tool call.
        include_transaction_tools: Add txn_* management tools.
        **react_agent_kwargs: Passed through to ``create_react_agent``.

    Returns:
        A compiled LangGraph ``CompiledStateGraph``.
    """
    # Collect plain tool list from ToolNode or sequence
    if isinstance(tools, ToolNode):
        user_tools: list[BaseTool | Callable] = list(tools.tools_by_name.values())
    else:
        user_tools = list(tools)

    # Add transaction management tools
    if include_transaction_tools:
        txn_tools = create_transaction_tools(coordinator)
        all_tools: list[BaseTool | Callable] = user_tools + txn_tools
    else:
        all_tools = list(user_tools)

    # Create ToolNode with transactional wrapper
    wrapper = TransactionalToolWrapper(
        coordinator, auto_savepoint=auto_savepoint
    )
    tool_node = ToolNode(all_tools, wrap_tool_call=wrapper)

    return create_react_agent(
        model, tool_node, **react_agent_kwargs
    )
