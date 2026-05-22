"""SessionManager — transaction lifecycle for coding sessions.

Manages begin/commit/abort of Janus transactions, session metadata,
and the bridge between the agent layer and the Janus shim infrastructure.

Every begin/commit/abort/savepoint call delegates to JanusContext and
its OverlayFS + SQLite MVCC shims, giving true transactional isolation.
"""

from __future__ import annotations

import logging
import threading
import uuid
from itertools import count
from pathlib import Path
from typing import Any

from janus_code.config import Config
from janus_code.context.conversation import Message
from janus_code.context.message_store import MessageStore

logger = logging.getLogger(__name__)


class TxnContext:
    """Wrapper around a transaction with convenience methods.

    Delegates lifecycle operations to JanusContext, including savepoints
    for subtransactions.
    """

    def __init__(
        self,
        txn_id: str,
        txn_handle: Any | None = None,
        janus_context: Any = None,
    ) -> None:
        self.txn_id = txn_id
        self._handle = txn_handle
        self._janus_context = janus_context
        # A subtransaction has a non-None parent_id on its handle
        self._is_subtxn: bool = (
            txn_handle is not None
            and getattr(txn_handle, "parent_id", None) is not None
        )
        self.is_active = True
        self._children: list[TxnContext] = []
        self._committed = False
        self._child_seq = 0

    @property
    def handle(self) -> Any:
        return self._handle

    def begin_subtxn(self, name: str = "") -> "TxnContext":
        """Begin a child subtransaction for sequential sub-agent work.

        When a JanusContext is present, creates a real OverlayFS savepoint
        so the sub-agent's writes are isolated. Tools are automatically
        switched to the child overlay by JanusContext.savepoint().
        """
        self._child_seq += 1
        child_id = f"{self.txn_id}_{self._child_seq}"

        child_handle = None
        if self._janus_context and self._janus_context.is_active:
            # Creates child overlay + switches all tools to child workdir
            self._janus_context.savepoint(name or child_id)
            # The handle for the child is tracked inside JanusContext
            child_handle = self._janus_context._child_txn

        child = TxnContext(
            txn_id=child_id,
            txn_handle=child_handle,
            janus_context=self._janus_context,  # share the same JanusContext
        )
        # Mark the child as a subtransaction
        child._is_subtxn = True

        self._children.append(child)
        logger.info("Subtxn %s begun (parent=%s)", child_id, self.txn_id)
        return child

    def commit(self) -> None:
        """Commit this transaction (or child into parent)."""
        if not self.is_active:
            return

        # Abort any active children first
        for child in self._children:
            if child.is_active:
                child.abort()

        if self._janus_context:
            if self._is_subtxn:
                # Child may already be closed externally; treat as finalized.
                if self._janus_context._child_txn is None:
                    logger.warning(
                        "Subtxn %s already closed before commit(); marking inactive.",
                        self.txn_id,
                    )
                    self.is_active = False
                    self._committed = True
                    return
                # Commit the active savepoint into the parent overlay
                self._janus_context._commit_active_savepoint()
            else:
                # Top-level txn may already be committed/aborted externally.
                if not self._janus_context.is_active:
                    logger.warning(
                        "Txn %s already closed before commit(); marking inactive.",
                        self.txn_id,
                    )
                    self.is_active = False
                    self._committed = True
                    return
                # Top-level commit: merge overlay to real project
                self._janus_context.commit()

        self.is_active = False
        self._committed = True
        logger.info("Txn %s committed", self.txn_id)

    def abort(self) -> None:
        """Abort this transaction. All changes discarded."""
        if not self.is_active:
            return

        # Abort children first
        for child in self._children:
            if child.is_active:
                child.abort()

        if self._janus_context:
            if self._is_subtxn:
                # Rollback the active savepoint (discard child overlay)
                if self._janus_context._child_txn is not None:
                    self._janus_context.rollback()
            else:
                # Top-level abort: discard all overlay changes
                if self._janus_context.is_active:
                    self._janus_context.abort()

        self.is_active = False
        logger.info("Txn %s aborted", self.txn_id)


class SessionManager:
    """Manages transaction lifecycles for a coding session.

    Provides two execution paths:
    - begin_txn/commit_txn/abort_txn: for inline (single-txn) execution.
    - begin_parallel_txns: for launching N concurrent transactions.

    Between committed transactions, state is durable. On crash, resume
    from the last committed transaction.

    Args:
        project_path: Root directory of the project being edited.
        config: Runtime configuration.
        janus_context: JanusContext instance used for all transactions.
        db_path: SQLite database path for the MessageStore.
    """
    _GLOBAL_TXN_COUNTER = count(1)
    _GLOBAL_TXN_LOCK = threading.Lock()

    @classmethod
    def _next_txn_id(cls) -> str:
        """Allocate a process-global sequential transaction id."""
        with cls._GLOBAL_TXN_LOCK:
            return f"txn{next(cls._GLOBAL_TXN_COUNTER)}"

    def __init__(
        self,
        project_path: Path,
        config: Config,
        janus_context: Any,
        db_path: str = ":memory:",
    ) -> None:
        if janus_context is None:
            raise ValueError("SessionManager requires an active JanusContext.")
        self.project_path = project_path
        self.config = config
        self.janus_context = janus_context
        self.session_id: str = ""
        self.turn_number: int = 0
        self._current_txn: TxnContext | None = None

        # Message persistence always participates in Janus's 2PC via SQLiteShim.
        shim = getattr(janus_context, "_sqlite_shim", None)
        if shim is None:
            raise ValueError(
                "SessionManager requires JanusContext with SQLiteShim enabled."
            )
        self.message_store = MessageStore(db_path, shim=shim)

    @property
    def current_txn(self) -> TxnContext | None:
        return self._current_txn

    def start_session(self) -> str:
        """Initialize a new session. Returns session_id.

        In Janus mode, the session metadata row is written in a brief
        bootstrap transaction that commits immediately, so the row is
        durably visible before the first user turn begins.
        """
        self.session_id = uuid.uuid4().hex[:12]
        self.turn_number = 0

        # Bootstrap: open a lightweight txn just to write the session row.
        bootstrap_handle = self.janus_context.begin()
        self.message_store.set_txn(bootstrap_handle)
        self.message_store.create_session(
            self.session_id, str(self.project_path)
        )
        self.janus_context.commit()
        self.message_store.set_txn(None)

        logger.info("Session %s started at %s", self.session_id, self.project_path)
        return self.session_id

    def resume_session(self, session_id: str) -> list[Message]:
        """Resume from committed state. Loads message history.

        In Janus mode, opens a read-only transaction to query committed
        messages, then aborts (no writes, no side-effects).
        """
        self.session_id = session_id
        messages = self._load_messages_for_session(session_id)

        if messages:
            self.turn_number = max(m.turn for m in messages) + 1
        else:
            self.turn_number = 0
        logger.info(
            "Session %s resumed at turn %d (%d messages)",
            session_id, self.turn_number, len(messages)
        )
        return messages

    def _load_messages_for_session(self, session_id: str) -> list[Message]:
        """Load session messages, opening a short read txn in Janus mode if needed."""
        if self.message_store._txn is None:
            bootstrap_handle = self.janus_context.begin()
            self.message_store.set_txn(bootstrap_handle)
            try:
                return self.message_store.load_messages(session_id)
            finally:
                if self.janus_context.is_active:
                    self.janus_context.abort()
                self.message_store.set_txn(None)
        return self.message_store.load_messages(session_id)

    # ── Inline (single-txn) path ─────────────────────────────────────

    def begin_txn(self) -> TxnContext:
        """Begin a transaction for the current turn.

        If a JanusContext was provided, calls janus_context.begin() which
        mounts the OverlayFS overlay and creates transactional tools.
        Also wires the resulting TransactionHandle into MessageStore so
        that all message reads/writes in this turn use the MVCC protocol.
        """
        self.turn_number += 1
        txn_id = self._next_txn_id()

        # begin() mounts the overlay; returns the TransactionHandle
        txn_handle = self.janus_context.begin()
        # Wire the handle into the message store for MVCC isolation
        self.message_store.set_txn(txn_handle)

        txn = TxnContext(
            txn_id=txn_id,
            txn_handle=txn_handle,
            janus_context=self.janus_context,
        )
        self._current_txn = txn
        logger.info("Txn %s begun (turn %d)", txn_id, self.turn_number)
        return txn

    def commit_txn(self, txn: TxnContext | None = None) -> None:
        """Commit the current transaction.

        The session-turn metadata write happens inside the transaction so
        it is atomic with the rest of the turn's state (messages, files).
        The MessageStore txn handle is cleared after commit.
        """
        txn = txn or self._current_txn
        if txn is None:
            raise RuntimeError("No active transaction to commit")
        # Write session-turn metadata before committing (inside the txn)
        if self.message_store._txn is not None:
            self.message_store.update_session_turn(
                self.session_id, self.turn_number
            )
        txn.commit()
        # Clear stale txn handle from message store
        self.message_store.set_txn(None)
        if txn is self._current_txn:
            self._current_txn = None

    def abort_txn(self, txn: TxnContext | None = None) -> None:
        """Abort the current transaction. All changes discarded.

        In Janus mode the MVCC shim eagerly undoes the message rows written
        during this turn, so they vanish from the store.  The MessageStore
        txn handle is cleared after abort.
        """
        txn = txn or self._current_txn
        if txn is None:
            return
        txn.abort()
        # Clear stale txn handle from message store
        self.message_store.set_txn(None)
        if txn is self._current_txn:
            self._current_txn = None

    # ── Parallel path ────────────────────────────────────────────────

    def begin_parallel_txns(self, n: int) -> list[TxnContext]:
        """Begin N independent transactions for parallel execution.

        Each transaction gets its own overlay and SQLite scope.
        They are peers, not parent-child. They CAN conflict.

        Note: for true concurrent overlays, use per-task JanusContext
        instances rather than sharing one SessionManager.
        """
        txns: list[TxnContext] = []
        for i in range(n):
            self.turn_number += 1
            txn_id = self._next_txn_id()

            txn_handle = self.janus_context.begin()

            txns.append(
                TxnContext(
                    txn_id=txn_id,
                    txn_handle=txn_handle,
                    janus_context=self.janus_context,
                )
            )
        return txns

    def begin_aggregator_txn(self) -> TxnContext:
        """Begin the aggregator transaction.

        Runs after all parallel transactions have committed.
        Reads committed results, merges, and produces final state.
        """
        self.turn_number += 1
        txn_id = self._next_txn_id()

        txn_handle = self.janus_context.begin()
        self.message_store.set_txn(txn_handle)

        txn = TxnContext(
            txn_id=txn_id,
            txn_handle=txn_handle,
            janus_context=self.janus_context,
        )
        self._current_txn = txn
        return txn

    # ── Persistence helpers ──────────────────────────────────────────

    def persist_messages(self, messages: list[Message]) -> None:
        """Persist messages to the store."""
        if self.message_store._txn is None:
            bootstrap_handle = self.janus_context.begin()
            self.message_store.set_txn(bootstrap_handle)
            try:
                self.message_store.store_messages(self.session_id, messages)
                self.janus_context.commit()
            except Exception:
                if self.janus_context.is_active:
                    self.janus_context.abort()
                raise
            finally:
                self.message_store.set_txn(None)
            return
        self.message_store.store_messages(self.session_id, messages)

    def persist_message(self, message: Message) -> None:
        """Persist a single message."""
        if self.message_store._txn is None:
            bootstrap_handle = self.janus_context.begin()
            self.message_store.set_txn(bootstrap_handle)
            try:
                self.message_store.store_message(self.session_id, message)
                self.janus_context.commit()
            except Exception:
                if self.janus_context.is_active:
                    self.janus_context.abort()
                raise
            finally:
                self.message_store.set_txn(None)
            return
        self.message_store.store_message(self.session_id, message)

    def load_messages(self) -> list[Message]:
        """Load all messages for current session."""
        return self._load_messages_for_session(self.session_id)

    def get_status(self) -> dict[str, Any]:
        """Get current session status."""
        return {
            "session_id": self.session_id,
            "turn_number": self.turn_number,
            "has_active_txn": self._current_txn is not None
            and self._current_txn.is_active,
            "project_path": str(self.project_path),
            "tar_mode": True,
        }
