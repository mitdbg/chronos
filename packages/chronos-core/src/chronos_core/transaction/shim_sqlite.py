"""SQLite MVCC shim for transactional virtual branching.

Implements the Epoxy-style MVCC protocol on top of SQLite:
  - Every user table is augmented with _begin_txn / _end_txn columns.
  - Writes tag new record versions with beginTxn = txn.numeric_id.
  - Updates set endTxn on the old version and insert a new version.
  - Reads apply the visibility predicate from TxnSnapshot.
  - Commit is coordinator-only (no per-record work for MVCC shims).
  - Abort leaves dead versions for GC.

Subtransaction support (1-level nesting):
  - Child writes use child.numeric_id as beginTxn.
  - On child commit: parent.snapshot.commit_child(child_id) — O(1).
    Child's records become visible to parent via self_set membership.
  - On child abort: parent.snapshot.abort_child(child_id) — O(1).
    Child's records become invisible to parent automatically.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from typing import Any, Callable

from chronos_core.transaction.shim import ToolShim
from chronos_core.transaction.types import (
    ChangeRecord,
    ChangeType,
    Savepoint,
    TransactionHandle,
    TxnSnapshot,
    Vote,
)

logger = logging.getLogger(__name__)

# Sentinel for live records (no end_txn)
_END_TXN_LIVE = 0


class WriteConflictError(Exception):
    """Raised when an exclusive write lock cannot be acquired.

    Follows the Epoxy eager-abort rule (Algorithm 1 §3.3): if a transaction
    cannot acquire a per-key write lock because another transaction holds it,
    it is immediately aborted rather than blocked (preventing deadlocks).
    """

    pass


class SQLiteShim(ToolShim):
    """MVCC-based SQLite shim for transactional virtual branching.

    Each registered table gets ``_begin_txn`` and ``_end_txn`` columns.
    Reads use the visibility predicate; writes tag new versions.

    Example::

        shim = SQLiteShim(":memory:")
        shim.register_table("users", ["id TEXT PRIMARY KEY", "name TEXT", "email TEXT"])

        txn = TransactionHandle.create()
        txn.snapshot = TxnSnapshot(xmin=0, committed_set=frozenset(), self_set={txn.numeric_id})
        shim.begin(txn)

        shim.put(txn, "users", {"id": "1", "name": "Alice", "email": "a@b.com"})
        row = shim.get(txn, "users", "1")
        assert row["name"] == "Alice"
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        enforce_write_locks: bool = True,
        enforce_snapshot_validation: bool | None = None,
    ):
        self._db_path = db_path
        self._enforce_write_locks = bool(enforce_write_locks)
        if enforce_snapshot_validation is None:
            # Weak mode commonly disables write locks; match that by disabling
            # first-committer-wins validation as well unless explicitly set.
            self._enforce_snapshot_validation = bool(enforce_write_locks)
        else:
            self._enforce_snapshot_validation = bool(enforce_snapshot_validation)
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()
        # table_name -> {"columns": [...], "pk": "col_name"}
        self._tables: dict[str, _TableMeta] = {}
        # txn_id -> set of (table, pk_value) modified
        self._txn_write_sets: dict[str, set[tuple[str, str]]] = {}

        # ── Per-key write lock manager (Epoxy §3.3) ──────────────────
        # Maps (table, pk_val) -> txn_id that holds the exclusive write lock.
        self._key_lock_holders: dict[tuple[str, str], str] = {}
        # Maps txn_id -> set of (table, pk_val) keys locked by that txn.
        self._txn_locked_keys: dict[str, set[tuple[str, str]]] = {}
        # Maps txn_id -> root txn_id for that transaction tree.
        self._txn_root_ids: dict[str, str] = {}
        # Mutex protecting the lock-manager dicts above.
        self._lock_mgr = threading.Lock()
        # Numeric IDs known to be committed for this shim.
        # Seed rows default to begin_txn=0, which is always committed.
        self._committed_numeric_ids: set[int] = {0}

        self._ensure_connection()

    def _ensure_connection(self) -> None:
        if self._conn is None:
            self._conn = sqlite3.connect(
                self._db_path, check_same_thread=False
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=OFF")

    @property
    def shim_id(self) -> str:
        return f"sqlite:{self._db_path}"

    @property
    def conn(self) -> sqlite3.Connection:
        self._ensure_connection()
        assert self._conn is not None
        return self._conn

    # ── Table registration ───────────────────────────────────────────

    def register_table(
        self,
        table_name: str,
        columns: list[str],
        pk_column: str | None = None,
    ) -> None:
        """Register (and create if needed) a table with MVCC columns.

        Args:
            table_name: Name of the table.
            columns: Column definitions, e.g. ["id TEXT", "name TEXT"].
                     The first column is the PK unless pk_column is given.
            pk_column: Explicit primary key column name.
        """
        # Parse column names from definitions
        col_names = []
        for c in columns:
            parts = c.strip().split()
            col_names.append(parts[0])

        pk = pk_column or col_names[0]
        meta = _TableMeta(
            name=table_name,
            columns=col_names,
            column_defs=columns,
            pk=pk,
        )
        self._tables[table_name] = meta

        # Create table with MVCC columns
        all_cols = columns + [
            "_begin_txn INTEGER NOT NULL DEFAULT 0",
            "_end_txn INTEGER NOT NULL DEFAULT 0",
            "_row_version INTEGER NOT NULL DEFAULT 0",
        ]
        col_sql = ", ".join(all_cols)
        with self._lock:
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} ({col_sql})"
            )
            # Index on MVCC columns for predicate push-down
            self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_mvcc "
                f"ON {table_name}(_begin_txn, _end_txn)"
            )
            self.conn.commit()

    def seed_data(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        committed_txn_id: int = 0,
    ) -> None:
        """Insert seed data as already-committed (beginTxn=0, endTxn=0=live).

        Useful for setting up initial state that all transactions can see.
        The committed_txn_id=0 means "committed before all snapshots".
        """
        meta = self._tables[table_name]
        cols = meta.columns + ["_begin_txn", "_end_txn", "_row_version"]
        placeholders = ", ".join("?" * len(cols))
        col_str = ", ".join(cols)
        with self._lock:
            for row in rows:
                vals = [row.get(c) for c in meta.columns]
                vals += [committed_txn_id, _END_TXN_LIVE, 0]
                self.conn.execute(
                    f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})",
                    vals,
                )
            self.conn.commit()
        self._committed_numeric_ids.add(committed_txn_id)

    # ── Per-key write lock manager (Epoxy §3.3) ────────────────────

    def _acquire_write_lock(
        self, txn: TransactionHandle, table: str, pk_val: str
    ) -> None:
        """Acquire an exclusive write lock on (table, pk_val) for txn.

        Implements the Epoxy write-lock rule (Algorithm 1, lines 12 & 17):
        - If the key is unlocked: acquire immediately.
        - If the lock is already held by *this* transaction: re-entrant, OK.
        - If the lock is held by the transaction's direct parent: the child
          is part of the same logical transaction scope, so it is allowed to
          write without taking ownership (parent retains the lock).
        - If the lock is held by any other transaction: conflict → raise
          WriteConflictError so the caller can abort the transaction.

        Args:
            txn: The transaction requesting the lock.
            table: Table name.
            pk_val: Primary-key value (as string).

        Raises:
            WriteConflictError: If the key is locked by a conflicting txn.
        """
        if not self._enforce_write_locks:
            return
        key = (table, pk_val)
        with self._lock_mgr:
            holder = self._key_lock_holders.get(key)
            requester_root = self._txn_root_ids.get(txn.id, txn.id)
            if holder is None:
                # Key is free — acquire the lock.
                self._key_lock_holders[key] = txn.id
                self._txn_locked_keys.setdefault(txn.id, set()).add(key)
            elif holder == txn.id:
                # Re-entrant: this txn already holds the lock.
                pass
            elif txn.parent_id is not None and holder == txn.parent_id:
                # Parent holds the lock; the child writes within the same
                # transaction tree and is allowed to proceed.  Ownership
                # stays with the parent so it is released when the parent
                # commits or aborts.
                pass
            elif self._txn_root_ids.get(holder, holder) == requester_root:
                # Sibling/descendant under the same logical root transaction:
                # allow speculative branches to touch the same key without
                # eager-aborting. Transfer lock ownership to current writer.
                self._key_lock_holders[key] = txn.id
                self._txn_locked_keys.setdefault(txn.id, set()).add(key)
            else:
                raise WriteConflictError(
                    f"Write conflict on {table}[{pk_val}]: "
                    f"txn {txn.id!r} cannot acquire lock held by {holder!r}"
                )

    def _release_write_locks(self, txn_id: str) -> None:
        """Release all write locks held by txn_id.

        Called on commit and abort (Algorithm 1, lines 37 & 46).
        """
        with self._lock_mgr:
            keys = self._txn_locked_keys.pop(txn_id, set())
            for key in keys:
                if self._key_lock_holders.get(key) == txn_id:
                    del self._key_lock_holders[key]

    def _transfer_write_locks(self, from_txn_id: str, to_txn_id: str) -> None:
        """Transfer all write locks from *from_txn_id* to *to_txn_id*.

        Used when a child transaction commits into its parent: the parent
        inherits responsibility for the keys the child locked so they remain
        protected until the parent itself commits or aborts.
        """
        with self._lock_mgr:
            keys = self._txn_locked_keys.pop(from_txn_id, set())
            if not keys:
                return
            parent_keys = self._txn_locked_keys.setdefault(to_txn_id, set())
            for key in keys:
                if self._key_lock_holders.get(key) == from_txn_id:
                    self._key_lock_holders[key] = to_txn_id
                parent_keys.add(key)

    # ── ToolShim interface ───────────────────────────────────────────

    def begin(self, txn: TransactionHandle) -> None:
        self._txn_write_sets[txn.id] = set()
        self._txn_locked_keys.setdefault(txn.id, set())
        root_id = txn.id
        if txn.parent_id is not None:
            root_id = self._txn_root_ids.get(txn.parent_id, txn.parent_id)
        self._txn_root_ids[txn.id] = root_id

    def prepare(self, txn: TransactionHandle) -> Vote:
        if not self._enforce_snapshot_validation:
            return Vote.COMMIT
        # First-committer-wins validation (snapshot isolation):
        # if any key in our write-set was written by a transaction that
        # committed after our snapshot, we must abort.
        #
        # Why this is needed in addition to write locks:
        # - Write locks are acquired at write time (put/delete), not read time.
        # - A stale txn can read old data, then later acquire the lock after
        #   the first writer commits and releases it.
        # - Without this check, both can commit, causing lost updates.
        ws = self._txn_write_sets.get(txn.id, set())
        if not ws or txn.snapshot is None:
            return Vote.COMMIT

        snap = txn.snapshot

        with self._lock:
            for table_name, pk_val in ws:
                meta = self._tables.get(table_name)
                if meta is None:
                    continue
                cur = self.conn.execute(
                    f"SELECT _begin_txn, _end_txn FROM {table_name} "
                    f"WHERE {meta.pk} = ?",
                    [pk_val],
                )
                for row in cur.fetchall():
                    begin_txn = int(row["_begin_txn"])
                    end_txn = int(row["_end_txn"] or 0)

                    # Any non-visible committed begin/end marker on this key
                    # means someone committed a write after our snapshot.
                    if (
                        begin_txn != 0
                        and begin_txn in self._committed_numeric_ids
                        and not self._txn_id_visible_in_snapshot(snap, begin_txn)
                    ):
                        logger.debug(
                            "prepare ABORT: stale write on %s[%s] due to begin_txn=%s",
                            table_name,
                            pk_val,
                            begin_txn,
                        )
                        return Vote.ABORT

                    if (
                        end_txn != 0
                        and end_txn in self._committed_numeric_ids
                        and not self._txn_id_visible_in_snapshot(snap, end_txn)
                    ):
                        logger.debug(
                            "prepare ABORT: stale write on %s[%s] due to end_txn=%s",
                            table_name,
                            pk_val,
                            end_txn,
                        )
                        return Vote.ABORT

        return Vote.COMMIT

    @staticmethod
    def _txn_id_visible_in_snapshot(snapshot: TxnSnapshot, txn_id: int) -> bool:
        return (
            txn_id < snapshot.xmin
            or txn_id in snapshot.committed_set
            or txn_id in snapshot.self_set
        )

    def commit(self, txn: TransactionHandle) -> None:
        """Commit: release write locks and clean up write-set metadata.

        The coordinator records this txn as committed. Future snapshots
        will include this txn's numeric_id in their committed_set, making
        all records with beginTxn = this txn visible.  Write locks are
        released here (Algorithm 1, line 37).
        """
        self._txn_write_sets.pop(txn.id, None)
        self._committed_numeric_ids.add(txn.numeric_id)
        if txn.snapshot:
            self._committed_numeric_ids.update(txn.snapshot.self_set)
        self._release_write_locks(txn.id)
        self._txn_root_ids.pop(txn.id, None)

    def abort(self, txn: TransactionHandle) -> None:
        """Abort: release write locks and eagerly undo MVCC changes.

        Records with beginTxn = this txn become permanently invisible
        (no future snapshot will include an aborted txn). GC reclaims them.

        We DO eagerly reset endTxn on records this txn superseded to restore
        visibility immediately.  Write locks are released after the undo so
        that concurrent transactions see a consistent state as soon as the
        lock is dropped (Algorithm 1, lines 38-46).
        """
        ws = self._txn_write_sets.pop(txn.id, None)
        if not ws:
            self._release_write_locks(txn.id)
            self._txn_root_ids.pop(txn.id, None)
            return

        # Eagerly restore endTxn for superseded records
        with self._lock:
            for table_name, pk_val in ws:
                meta = self._tables.get(table_name)
                if not meta:
                    continue
                # Reset endTxn on records this txn superseded
                self.conn.execute(
                    f"UPDATE {table_name} SET _end_txn = 0 "
                    f"WHERE _end_txn = ? AND {meta.pk} = ?",
                    [txn.numeric_id, pk_val],
                )
                # Delete records this txn created
                self.conn.execute(
                    f"DELETE FROM {table_name} "
                    f"WHERE _begin_txn = ? AND {meta.pk} = ?",
                    [txn.numeric_id, pk_val],
                )
            self.conn.commit()

        # Release write locks after the undo is durable (Algorithm 1, line 46)
        self._release_write_locks(txn.id)
        self._txn_root_ids.pop(txn.id, None)

    def savepoint(self, txn: TransactionHandle, sp: Savepoint) -> Any:
        """Savepoint: snapshot the current write set for rollback."""
        ws = self._txn_write_sets.get(txn.id, set())
        return ws.copy()

    def rollback_to_savepoint(
        self, txn: TransactionHandle, sp: Savepoint
    ) -> None:
        """Rollback to savepoint: undo writes made after the savepoint.

        For MVCC with subtransactions, the coordinator handles self_set
        management. Here we do the physical cleanup of rows written
        after the savepoint.
        """
        saved_ws = sp.shim_snapshots.get(self.shim_id)
        if saved_ws is None:
            saved_ws = set()

        current_ws = self._txn_write_sets.get(txn.id, set())
        new_writes = current_ws - saved_ws

        with self._lock:
            for table_name, pk_val in new_writes:
                meta = self._tables.get(table_name)
                if not meta:
                    continue
                # Find all numeric IDs in the txn's self_set to undo
                if txn.snapshot:
                    for nid in txn.snapshot.self_set:
                        self.conn.execute(
                            f"UPDATE {table_name} SET _end_txn = 0 "
                            f"WHERE _end_txn = ? AND {meta.pk} = ?",
                            [nid, pk_val],
                        )
                        self.conn.execute(
                            f"DELETE FROM {table_name} "
                            f"WHERE _begin_txn = ? AND {meta.pk} = ?",
                            [nid, pk_val],
                        )
            self.conn.commit()

        self._txn_write_sets[txn.id] = saved_ws.copy()

    def get_changes(self, txn: TransactionHandle) -> list[ChangeRecord]:
        ws = self._txn_write_sets.get(txn.id, set())
        changes: list[ChangeRecord] = []
        for table, pk_val in ws:
            changes.append(
                ChangeRecord(
                    shim_id=self.shim_id,
                    resource_id=f"{table}/{pk_val}",
                    change_type=ChangeType.UPDATE,
                )
            )
        return changes

    # ── Data operations ──────────────────────────────────────────────

    def put(
        self,
        txn: TransactionHandle,
        table: str,
        row: dict[str, Any],
    ) -> None:
        """Insert or update a row within a transaction.

        MVCC write protocol:
        1. Find the currently visible version of this PK (if any).
        2. If found: set endTxn = txn.numeric_id on the old version.
        3. Insert a new version with beginTxn = txn.numeric_id.
        """
        meta = self._tables[table]
        pk_val = str(row[meta.pk])
        write_txn_id = txn.numeric_id

        # Acquire exclusive write lock before any MVCC work (Epoxy §3.3).
        # Raises WriteConflictError immediately if another txn holds the lock.
        self._acquire_write_lock(txn, table, pk_val)

        with self._lock:
            # Find currently visible version and supersede it
            if txn.snapshot:
                vis_clause, vis_params = txn.snapshot.visibility_sql()
                cur = self.conn.execute(
                    f"SELECT rowid, _begin_txn, _end_txn FROM {table} "
                    f"WHERE {meta.pk} = ? AND {vis_clause} "
                    f"ORDER BY _begin_txn DESC LIMIT 1",
                    [pk_val] + vis_params,
                )
                existing = cur.fetchone()
                if existing:
                    existing_end = int(existing["_end_txn"] or 0)
                    # If the row appears visible only because it was ended by
                    # a committed-but-not-visible txn, this is a stale writer.
                    if (
                        self._enforce_snapshot_validation
                        and (
                        existing_end != 0
                        and existing_end in self._committed_numeric_ids
                        and not self._txn_id_visible_in_snapshot(
                            txn.snapshot, existing_end
                        )
                        )
                    ):
                        raise WriteConflictError(
                            f"Stale write conflict on {table}[{pk_val}]: "
                            f"version ended by committed txn {existing_end}"
                        )
                    self.conn.execute(
                        f"UPDATE {table} SET _end_txn = ? WHERE rowid = ?",
                        [write_txn_id, existing["rowid"]],
                    )

            # Insert new version
            cols = meta.columns + ["_begin_txn", "_end_txn", "_row_version"]
            vals = [row.get(c) for c in meta.columns]
            vals += [write_txn_id, _END_TXN_LIVE, 0]
            placeholders = ", ".join("?" * len(cols))
            col_str = ", ".join(cols)
            self.conn.execute(
                f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})",
                vals,
            )
            self.conn.commit()

        ws = self._txn_write_sets.setdefault(txn.id, set())
        ws.add((table, pk_val))

    def update(
        self,
        txn: TransactionHandle,
        table: str,
        pk_val: str,
        updates: dict[str, Any | Callable[[Any], Any] | Callable[[Any, dict[str, Any]], Any]]
        | None = None,
        updater: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
        create_if_missing: bool = False,
    ) -> dict[str, Any] | None:
        """Read-modify-write a row through MVCC-safe get/put.

        This is the preferred interface for in-transaction updates that would
        otherwise be expressed as raw SQL UPDATE statements.

        Args:
            txn: Active transaction.
            table: Table name.
            pk_val: Primary-key value of the row to update.
            updates: Optional partial update mapping. Values may be literals or
                callables. Callable values receive either:
                - current column value: ``fn(current_value)``, or
                - current column value + whole row: ``fn(current_value, row)``.
            updater: Optional custom transformer that receives a row dict and
                returns the updated row (or None to use in-place mutations).
            create_if_missing: If True and the row is missing, create a new
                row initialized with just the PK before applying updates.

        Returns:
            Updated row dict, or None if row is missing and create_if_missing
            is False.

        Raises:
            ValueError: If both updates and updater are omitted, or if the
                updater changes/removes the PK.
            WriteConflictError: On normal MVCC write conflicts via put().
        """
        if updates is None and updater is None:
            raise ValueError("update() requires 'updates' and/or 'updater'")

        meta = self._tables[table]
        row = self.get(txn, table, pk_val)
        if row is None:
            if not create_if_missing:
                return None
            row = {meta.pk: pk_val}

        next_row = dict(row)

        if updater is not None:
            candidate = updater(dict(next_row))
            if candidate is not None:
                if not isinstance(candidate, dict):
                    raise ValueError("update() updater must return a dict or None")
                next_row = dict(candidate)

        if updates:
            for col, value_or_fn in updates.items():
                if callable(value_or_fn):
                    current_val = next_row.get(col)
                    try:
                        next_row[col] = value_or_fn(current_val, dict(next_row))
                    except TypeError:
                        next_row[col] = value_or_fn(current_val)
                else:
                    next_row[col] = value_or_fn

        updated_pk = next_row.get(meta.pk)
        if updated_pk is None or str(updated_pk) != str(pk_val):
            raise ValueError(
                f"update() must preserve primary key {meta.pk}={pk_val!r}"
            )

        self.put(txn, table, next_row)
        return next_row

    def increment(
        self,
        txn: TransactionHandle,
        table: str,
        pk_val: str,
        field: str,
        delta: int | float = 1,
        *,
        default: int | float = 0,
        create_if_missing: bool = False,
    ) -> dict[str, Any] | None:
        """Increment a numeric field for one PK via MVCC version chaining.

        This helper exists to avoid raw SQL UPDATE writes that bypass MVCC.
        Unlike snapshot-based update(), increment() chains from the *current
        live head* (``_end_txn = 0``) under the shim lock, so each committed
        increment appends one new version and closes prior live versions.
        """
        if not isinstance(delta, (int, float)) or isinstance(delta, bool):
            raise ValueError("increment() requires numeric delta")
        if not isinstance(default, (int, float)) or isinstance(default, bool):
            raise ValueError("increment() requires numeric default")

        meta = self._tables[table]
        if field not in meta.columns:
            raise ValueError(f"Unknown column {field!r} for table {table!r}")
        if field == meta.pk:
            raise ValueError("increment() cannot target the primary-key column")

        write_txn_id = txn.numeric_id
        self._acquire_write_lock(txn, table, pk_val)

        with self._lock:
            cur = self.conn.execute(
                f"SELECT rowid, * FROM {table} "
                f"WHERE {meta.pk} = ? AND _end_txn = 0 "
                f"ORDER BY _begin_txn DESC",
                [pk_val],
            )
            live_rows = cur.fetchall()
            head = live_rows[0] if live_rows else None

            if head is None and not create_if_missing:
                return None

            if (
                head is not None
                and self._enforce_snapshot_validation
                and txn.snapshot is not None
            ):
                begin_txn = int(head["_begin_txn"] or 0)
                if (
                    begin_txn != 0
                    and begin_txn in self._committed_numeric_ids
                    and not self._txn_id_visible_in_snapshot(
                        txn.snapshot, begin_txn
                    )
                ):
                    raise WriteConflictError(
                        f"Stale increment conflict on {table}[{pk_val}]: "
                        f"head begin_txn={begin_txn} is not visible in snapshot"
                    )

            if head is None:
                row_data = {meta.pk: pk_val}
                old_value: int | float = default
            else:
                row_data = {col: head[col] for col in meta.columns}
                old_raw = row_data.get(field)
                old_value = default if old_raw is None else old_raw

            if not isinstance(old_value, (int, float)) or isinstance(old_value, bool):
                raise ValueError(
                    f"increment() target column {field!r} has non-numeric value {old_value!r}"
                )

            new_value = old_value + delta
            row_data[field] = new_value

            # Heal/extend chain: close all currently-live versions for this PK.
            if live_rows:
                for lr in live_rows:
                    self.conn.execute(
                        f"UPDATE {table} SET _end_txn = ? WHERE rowid = ?",
                        [write_txn_id, lr["rowid"]],
                    )

            cols = meta.columns + ["_begin_txn", "_end_txn", "_row_version"]
            vals = [row_data.get(c) for c in meta.columns]
            vals += [write_txn_id, _END_TXN_LIVE, 0]
            placeholders = ", ".join("?" * len(cols))
            col_str = ", ".join(cols)
            self.conn.execute(
                f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})",
                vals,
            )
            self.conn.commit()

        ws = self._txn_write_sets.setdefault(txn.id, set())
        ws.add((table, pk_val))
        return row_data

    def get(
        self,
        txn: TransactionHandle,
        table: str,
        pk_val: str,
    ) -> dict[str, Any] | None:
        """Read a single row by PK, applying the visibility predicate."""
        meta = self._tables[table]
        if txn.snapshot is None:
            return None

        vis_clause, vis_params = txn.snapshot.visibility_sql()
        with self._lock:
            cur = self.conn.execute(
                f"SELECT * FROM {table} "
                f"WHERE {meta.pk} = ? AND {vis_clause} "
                f"ORDER BY _begin_txn DESC LIMIT 1",
                [pk_val] + vis_params,
            )
            row = cur.fetchone()
        if row is None:
            return None
        return {col: row[col] for col in meta.columns}

    def query(
        self,
        txn: TransactionHandle,
        table: str,
        filters: dict[str, Any] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Query rows with visibility predicate and optional filters."""
        meta = self._tables[table]
        if txn.snapshot is None:
            return []

        vis_clause, vis_params = txn.snapshot.visibility_sql()

        where_parts = [vis_clause]
        params: list[Any] = list(vis_params)

        if filters:
            for col, val in filters.items():
                where_parts.append(f"{col} = ?")
                params.append(val)

        where_sql = " AND ".join(where_parts)

        sql = f"SELECT * FROM {table} WHERE {where_sql}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit:
            sql += f" LIMIT {limit}"

        with self._lock:
            cur = self.conn.execute(sql, params)
            rows = cur.fetchall()

        # Deduplicate by PK (keep highest beginTxn = latest version)
        seen: dict[str, dict[str, Any]] = {}
        for row in rows:
            pk = str(row[meta.pk])
            result = {col: row[col] for col in meta.columns}
            # Keep the version with the highest _begin_txn
            if pk not in seen or row["_begin_txn"] > seen[pk].get(
                "_begin_txn", -1
            ):
                result["_begin_txn"] = row["_begin_txn"]
                seen[pk] = result
        return [
            {k: v for k, v in r.items() if k != "_begin_txn"}
            for r in seen.values()
        ]

    def delete(
        self,
        txn: TransactionHandle,
        table: str,
        pk_val: str,
    ) -> bool:
        """Delete a row by setting endTxn on its visible version."""
        meta = self._tables[table]
        if txn.snapshot is None:
            return False

        vis_clause, vis_params = txn.snapshot.visibility_sql()

        # Acquire exclusive write lock before any MVCC work (Epoxy §3.3).
        # Raises WriteConflictError immediately if another txn holds the lock.
        self._acquire_write_lock(txn, table, pk_val)

        with self._lock:
            cur = self.conn.execute(
                f"SELECT rowid, _end_txn FROM {table} "
                f"WHERE {meta.pk} = ? AND {vis_clause} "
                f"ORDER BY _begin_txn DESC LIMIT 1",
                [pk_val] + vis_params,
            )
            existing = cur.fetchone()
            if existing is None:
                return False
            existing_end = int(existing["_end_txn"] or 0)
            if (
                self._enforce_snapshot_validation
                and (
                existing_end != 0
                and existing_end in self._committed_numeric_ids
                and not self._txn_id_visible_in_snapshot(txn.snapshot, existing_end)
                )
            ):
                raise WriteConflictError(
                    f"Stale delete conflict on {table}[{pk_val}]: "
                    f"version ended by committed txn {existing_end}"
                )
            self.conn.execute(
                f"UPDATE {table} SET _end_txn = ? WHERE rowid = ?",
                [txn.numeric_id, existing["rowid"]],
            )
            self.conn.commit()

        ws = self._txn_write_sets.setdefault(txn.id, set())
        ws.add((table, pk_val))
        return True

    def execute_sql(
        self,
        txn: TransactionHandle,
        sql: str,
        params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute raw SQL with visibility predicate injected.

        The SQL must be a SELECT. The visibility predicate is NOT
        auto-injected — use query() for that. This is for advanced
        queries where the caller manages visibility.
        """
        sql_norm = sql.strip().lower()
        if not sql_norm.startswith("select"):
            raise ValueError(
                "execute_sql only supports read-only SELECT queries"
            )

        with self._lock:
            cur = self.conn.execute(sql, params or [])
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    # ── Subtransaction support ───────────────────────────────────────

    def begin_child(
        self,
        child_txn: TransactionHandle,
        parent_txn: TransactionHandle | None = None,
    ) -> None:
        """Begin a child subtransaction. Initialize write set and lock tracking."""
        self._txn_write_sets[child_txn.id] = set()
        self._txn_locked_keys.setdefault(child_txn.id, set())
        root_id = child_txn.id
        if parent_txn is not None:
            root_id = self._txn_root_ids.get(parent_txn.id, parent_txn.id)
        self._txn_root_ids[child_txn.id] = root_id

    def commit_child(
        self,
        child_txn: TransactionHandle,
        parent_txn: TransactionHandle,
    ) -> None:
        """Commit child into parent — merge write sets and transfer write locks.

        The MVCC visibility predicate handles record visibility via self_set.
        Write locks acquired by the child are transferred to the parent so they
        remain held until the parent transaction commits or aborts.
        """
        child_ws = self._txn_write_sets.pop(child_txn.id, set())
        parent_ws = self._txn_write_sets.setdefault(parent_txn.id, set())
        parent_ws |= child_ws
        # Transfer write locks: child → parent
        self._transfer_write_locks(child_txn.id, parent_txn.id)
        self._txn_root_ids.pop(child_txn.id, None)

    def abort_child(
        self,
        child_txn: TransactionHandle,
        parent_txn: TransactionHandle,
    ) -> None:
        """Abort child — undo dead records and release child's write locks.

        Eagerly deletes records created by the child and restores endTxn on
        records the child superseded.  Write locks are released after the undo
        is durable so concurrent transactions see a consistent state immediately.
        """
        child_ws = self._txn_write_sets.pop(child_txn.id, set())
        with self._lock:
            for table_name, pk_val in child_ws:
                meta = self._tables.get(table_name)
                if not meta:
                    continue
                self.conn.execute(
                    f"UPDATE {table_name} SET _end_txn = 0 "
                    f"WHERE _end_txn = ? AND {meta.pk} = ?",
                    [child_txn.numeric_id, pk_val],
                )
                self.conn.execute(
                    f"DELETE FROM {table_name} "
                    f"WHERE _begin_txn = ? AND {meta.pk} = ?",
                    [child_txn.numeric_id, pk_val],
                )
            self.conn.commit()
        # Release child write locks after the undo is durable
        self._release_write_locks(child_txn.id)
        self._txn_root_ids.pop(child_txn.id, None)

    # ── GC ───────────────────────────────────────────────────────────

    def gc(self, committed_below: int) -> int:
        """Garbage-collect dead record versions.

        Removes records where:
        - endTxn != 0 AND endTxn < committed_below (superseded by committed txn)
        - beginTxn > 0 AND beginTxn not committed (aborted txn's creates)

        Args:
            committed_below: All txns with numeric_id < this are committed.

        Returns:
            Number of rows deleted.
        """
        total = 0
        with self._lock:
            for table_name in self._tables:
                cur = self.conn.execute(
                    f"DELETE FROM {table_name} "
                    f"WHERE _end_txn != 0 AND _end_txn < ?",
                    [committed_below],
                )
                total += cur.rowcount
            self.conn.commit()
        return total

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


class _TableMeta:
    """Metadata for a registered table."""

    __slots__ = ("name", "columns", "column_defs", "pk")

    def __init__(
        self,
        name: str,
        columns: list[str],
        column_defs: list[str],
        pk: str,
    ):
        self.name = name
        self.columns = columns
        self.column_defs = column_defs
        self.pk = pk
