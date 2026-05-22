"""MessageStore — shim-backed transactional conversation persistence.

All reads/writes go through SQLiteShim with MVCC visibility predicates and
participate in the same 2PC transaction as OverlayFS updates.
"""

from __future__ import annotations

import json
import time
from typing import Any

from janus_code.context.conversation import Conversation, Message, ToolCall

# ── Table schemas for SQLiteShim registration ───────────────────────
# First column is the primary key in each table.

_MESSAGES_COLS = [
    "msg_id TEXT",           # PK  (session_id:turn:seq)
    "session_id TEXT",
    "turn INTEGER",
    "role TEXT",
    "content TEXT",
    "tool_calls_json TEXT",
    "tool_call_id TEXT",
    "timestamp REAL",
    "created_at REAL",
]

_TOOL_CALLS_COLS = [
    "tc_id TEXT",            # PK  (session_id:turn:call_id:seq)
    "session_id TEXT",
    "turn INTEGER",
    "call_id TEXT",
    "tool_name TEXT",
    "args_json TEXT",
    "result_json TEXT",
    "duration_ms INTEGER",
    "timestamp REAL",
]

_TODOS_COLS = [
    "todo_key TEXT",         # PK  (session_id:todo_id)
    "session_id TEXT",
    "todo_id TEXT",
    "title TEXT",
    "status TEXT",
    "updated_at REAL",
]

_SESSIONS_COLS = [
    "session_id TEXT",       # PK
    "project_path TEXT",
    "created_at REAL",
    "last_turn INTEGER",
    "status TEXT",
]


class MessageStore:
    """SQLite-backed persistence for conversation messages and tool calls.

    Stores messages and tool calls with session scoping. All operations use
    the Janus SQLiteShim and require a current transaction handle.

    Args:
        db_path: Retained for compatibility with call sites; unused.
        shim: ``SQLiteShim`` from the active ``JanusContext``.
        txn: The current ``TransactionHandle``.  Must be set before any
             read/write. Updated by ``set_txn()`` at the
             start of each new turn.
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        shim: Any | None = None,
        txn: Any | None = None,
    ) -> None:
        self._shim = shim
        self._txn = txn
        # Per-process sequence counters for synthetic PKs (Janus mode).
        self._msg_seq = 0
        self._tc_seq = 0

        if shim is None:
            raise ValueError("MessageStore requires SQLiteShim; compatibility mode removed.")

        # Register tables with the MVCC shim.
        shim.register_table("messages", _MESSAGES_COLS, pk_column="msg_id")
        shim.register_table("tool_calls", _TOOL_CALLS_COLS, pk_column="tc_id")
        shim.register_table("todos", _TODOS_COLS, pk_column="todo_key")
        shim.register_table("sessions", _SESSIONS_COLS, pk_column="session_id")

    def set_txn(self, txn: Any) -> None:
        """Update the active transaction handle (called by SessionManager)."""
        self._txn = txn

    # ── Janus-mode PK helpers ──────────────────────────────────────────

    def _next_msg_id(self, session_id: str, turn: int) -> str:
        self._msg_seq += 1
        return f"{session_id}:msg:{turn}:{self._msg_seq}"

    def _next_tc_id(self, session_id: str, turn: int, call_id: str) -> str:
        self._tc_seq += 1
        return f"{session_id}:tc:{turn}:{call_id}:{self._tc_seq}"

    # ── Session management ───────────────────────────────────────────

    def create_session(self, session_id: str, project_path: str) -> None:
        """Register a new session."""
        assert self._txn is not None, "txn must be set before MessageStore ops"
        self._shim.put(self._txn, "sessions", {
            "session_id": session_id,
            "project_path": project_path,
            "created_at": time.time(),
            "last_turn": 0,
            "status": "active",
        })

    def update_session_turn(self, session_id: str, turn: int) -> None:
        """Update the last turn number for a session."""
        assert self._txn is not None
        existing = self._shim.get(self._txn, "sessions", session_id)
        if existing:
            existing["last_turn"] = turn
            self._shim.put(self._txn, "sessions", existing)

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        """Get session metadata."""
        assert self._txn is not None
        return self._shim.get(self._txn, "sessions", session_id)

    def list_sessions(self) -> list[dict[str, Any]]:
        """List all sessions."""
        assert self._txn is not None
        return self._shim.query(self._txn, "sessions", order_by="created_at")

    # ── Message persistence ──────────────────────────────────────────

    def store_message(self, session_id: str, message: Message) -> None:
        """Store a single message."""
        tool_calls_json = None
        if message.tool_calls:
            tool_calls_json = json.dumps(
                [tc.to_dict() for tc in message.tool_calls]
            )

        assert self._txn is not None
        msg_id = self._next_msg_id(session_id, message.turn)
        self._shim.put(self._txn, "messages", {
            "msg_id": msg_id,
            "session_id": session_id,
            "turn": message.turn,
            "role": message.role,
            "content": message.content or "",
            "tool_calls_json": tool_calls_json,
            "tool_call_id": message.tool_call_id,
            "timestamp": message.timestamp,
            "created_at": time.time(),
        })

    def store_messages(self, session_id: str, messages: list[Message]) -> None:
        """Store multiple messages in a batch."""
        for msg in messages:
            self.store_message(session_id, msg)

    def load_messages(self, session_id: str) -> list[Message]:
        """Load all messages for a session, ordered by turn and insertion order."""
        assert self._txn is not None
        rows = self._shim.query(
            self._txn, "messages",
            filters={"session_id": session_id},
        )
        # Sort by (turn, msg_id) — msg_id encodes the seq counter
        rows = sorted(rows, key=lambda r: (int(r.get("turn", 0)), r.get("msg_id", "")))

        messages: list[Message] = []
        for row in rows:
            tool_calls = None
            if row.get("tool_calls_json"):
                tool_calls = [
                    ToolCall.from_dict(tc)
                    for tc in json.loads(row["tool_calls_json"])
                ]
            messages.append(
                Message(
                    role=row["role"],
                    content=row.get("content") or "",
                    tool_calls=tool_calls,
                    tool_call_id=row.get("tool_call_id"),
                    turn=row["turn"],
                    timestamp=row["timestamp"],
                )
            )
        return messages

    def get_last_turn(self, session_id: str) -> int:
        """Get the last turn number for a session."""
        assert self._txn is not None
        rows = self._shim.query(
            self._txn, "messages", filters={"session_id": session_id}
        )
        if not rows:
            return 0
        return max(int(r.get("turn", 0)) for r in rows)

    # ── Tool call persistence ────────────────────────────────────────

    def store_tool_call(
        self,
        session_id: str,
        turn: int,
        tool_call: ToolCall,
    ) -> None:
        """Store a tool call record."""
        assert self._txn is not None
        tc_id = self._next_tc_id(session_id, turn, tool_call.id)
        self._shim.put(self._txn, "tool_calls", {
            "tc_id": tc_id,
            "session_id": session_id,
            "turn": turn,
            "call_id": tool_call.id,
            "tool_name": tool_call.name,
            "args_json": json.dumps(tool_call.arguments),
            "result_json": tool_call.result,
            "duration_ms": tool_call.duration_ms or 0,
            "timestamp": time.time(),
        })

    def load_tool_calls(
        self, session_id: str, turn: int | None = None
    ) -> list[ToolCall]:
        """Load tool calls, optionally filtered by turn."""
        assert self._txn is not None
        filters: dict[str, Any] = {"session_id": session_id}
        if turn is not None:
            filters["turn"] = turn
        rows = self._shim.query(self._txn, "tool_calls", filters=filters)
        rows = sorted(rows, key=lambda r: (int(r.get("turn", 0)), r.get("tc_id", "")))

        return [
            ToolCall(
                id=row["call_id"],
                name=row["tool_name"],
                arguments=json.loads(row["args_json"]),
                result=row.get("result_json"),
                duration_ms=row.get("duration_ms", 0),
            )
            for row in rows
        ]

    # ── Todo persistence ─────────────────────────────────────────────

    def store_todos(
        self, session_id: str, todos: list[dict[str, Any]]
    ) -> None:
        """Replace all todos for a session."""
        assert self._txn is not None
        # Delete existing todos for this session
        existing = self._shim.query(
            self._txn, "todos", filters={"session_id": session_id}
        )
        for row in existing:
            self._shim.delete(self._txn, "todos", row["todo_key"])
        # Insert replacements
        for t in todos:
            todo_key = f"{session_id}:{t['id']}"
            self._shim.put(self._txn, "todos", {
                "todo_key": todo_key,
                "session_id": session_id,
                "todo_id": t["id"],
                "title": t["title"],
                "status": t.get("status", "pending"),
                "updated_at": t.get("updated_at", time.time()),
            })

    def load_todos(self, session_id: str) -> list[dict[str, Any]]:
        """Load todos for a session."""
        assert self._txn is not None
        rows = self._shim.query(
            self._txn, "todos", filters={"session_id": session_id}
        )
        rows = sorted(rows, key=lambda r: r.get("todo_key", ""))

        return [
            {
                "id": row["todo_id"],
                "title": row["title"],
                "status": row["status"],
                "updated_at": row.get("updated_at", 0.0),
            }
            for row in rows
        ]

    def close(self) -> None:
        """No-op for shim-backed storage."""
        return None
