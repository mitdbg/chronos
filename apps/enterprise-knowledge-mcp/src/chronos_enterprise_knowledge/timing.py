"""Low-overhead timing hooks for replaying a poly-store workload.

The benchmark records time at storage-client boundaries rather than inferring
the store from the name of an application operation.  A context-local
collector makes the same instrumentation usable by both backend
implementations without changing the backend-neutral service API.
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import os
import threading
import time
from collections.abc import Iterator
from typing import Any

STORE_CATEGORIES = ("relational_db", "vector_db", "filesystem")

_ACTIVE_COLLECTOR: contextvars.ContextVar["StoreTimingCollector | None"] = (
    contextvars.ContextVar("chronos_store_timing", default=None)
)


class StoreTimingCollector:
    """Collect exclusive elapsed time for concrete store calls."""

    def __init__(self) -> None:
        self._totals_ns = {category: 0 for category in STORE_CATEGORIES}
        self._totals_lock = threading.Lock()
        self._local = threading.local()

    @property
    def _stack(self) -> list[dict[str, int | str]]:
        stack = getattr(self._local, "stack", None)
        if stack is None:
            stack = []
            self._local.stack = stack
        return stack

    @contextlib.contextmanager
    def active(self) -> Iterator["StoreTimingCollector"]:
        token = _ACTIVE_COLLECTOR.set(self)
        try:
            yield self
        finally:
            _ACTIVE_COLLECTOR.reset(token)

    @contextlib.contextmanager
    def span(self, category: str) -> Iterator[None]:
        if category not in STORE_CATEGORIES:
            raise ValueError(f"unknown store timing category: {category}")
        started = time.perf_counter_ns()
        frame: dict[str, int | str] = {
            "category": category,
            "started": started,
            "nested": 0,
        }
        self._stack.append(frame)
        try:
            yield
        finally:
            elapsed = time.perf_counter_ns() - started
            nested = int(frame["nested"])
            self._stack.pop()
            # A call may invoke another instrumented store.  Charge the
            # nested span to its own store and only the exclusive remainder to
            # the enclosing span.
            with self._totals_lock:
                self._totals_ns[category] += max(0, elapsed - nested)
            if self._stack:
                self._stack[-1]["nested"] = (
                    int(self._stack[-1]["nested"]) + elapsed
                )

    def snapshot_ms(self) -> dict[str, float]:
        with self._totals_lock:
            return {
                category: self._totals_ns[category] / 1_000_000
                for category in STORE_CATEGORIES
            }


def current_collector() -> StoreTimingCollector | None:
    return _ACTIVE_COLLECTOR.get()


@contextlib.contextmanager
def timed_store_call(category: str) -> Iterator[None]:
    collector = current_collector()
    if collector is None:
        yield
    else:
        with collector.span(category):
            yield


def _wrap_result(value: Any, category: str) -> Any:
    """Wrap DB cursors and context managers returned by store calls."""

    if value is None or isinstance(
        value,
        (str, bytes, bytearray, int, float, bool),
    ):
        return value
    # Filesystem clients commonly return pathlib.Path objects (for example,
    # a native branch checkout path).  They are values, not database cursors;
    # wrapping them would turn ``str(path)`` into the proxy repr and break
    # shell work-directory and workspace-token handling during replay.
    if isinstance(value, os.PathLike):
        return value
    if isinstance(value, (list, tuple, dict, set, frozenset)):
        return value
    if isinstance(value, TimedStoreProxy):
        return value
    # Rows and response objects are intentionally left alone.  Objects that
    # expose methods (DB cursors and transaction context managers) need their
    # later fetch/enter/exit work timed as well.
    if any(
        hasattr(value, name)
        for name in ("fetchone", "fetchall", "fetchmany", "__enter__", "__iter__")
    ):
        return TimedStoreProxy(value, category)
    return value


class TimedStoreProxy:
    """Transparent proxy that measures callable methods on one store client."""

    def __init__(self, value: Any, category: str):
        self._timed_value = value
        self._timed_category = category

    @property
    def raw_connection(self) -> Any:
        # Native Chronos branch stores need the driver connection itself.  It
        # is a handle, not a database operation, and must not be turned into
        # a callable proxy.
        return getattr(self._timed_value, "raw_connection")

    @property
    def database_url(self) -> Any:
        return getattr(self._timed_value, "database_url", None)

    @property
    def database_path(self) -> Any:
        return getattr(self._timed_value, "database_path", None)

    @property
    def dialect(self) -> Any:
        return getattr(self._timed_value, "dialect", None)

    @property
    def in_transaction(self) -> Any:
        return getattr(self._timed_value, "in_transaction", False)

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._timed_value, name)
        if not callable(attribute):
            return attribute

        def invoke(*args: Any, **kwargs: Any) -> Any:
            with timed_store_call(self._timed_category):
                result = attribute(*args, **kwargs)
            return _wrap_result(result, self._timed_category)

        return invoke

    def __enter__(self) -> Any:
        with timed_store_call(self._timed_category):
            result = self._timed_value.__enter__()
        return _wrap_result(result, self._timed_category)

    def __exit__(self, *args: Any) -> Any:
        with timed_store_call(self._timed_category):
            return self._timed_value.__exit__(*args)

    def __iter__(self) -> Iterator[Any]:
        def iterate() -> Iterator[Any]:
            with timed_store_call(self._timed_category):
                yield from self._timed_value

        return iterate()

    def __getitem__(self, key: Any) -> Any:
        with timed_store_call(self._timed_category):
            return self._timed_value[key]


def instrument_object(value: Any, category: str) -> TimedStoreProxy:
    """Return a proxy for one backend-owned store object."""

    if category not in STORE_CATEGORIES:
        raise ValueError(f"unknown store timing category: {category}")
    if isinstance(value, TimedStoreProxy):
        return value
    return TimedStoreProxy(value, category)


def instrument_backend(backend: Any) -> Any:
    """Attach timers to the concrete clients owned by an enterprise backend."""

    # The native baseline exposes one client per participating store.
    if hasattr(backend, "_db"):
        backend._db = instrument_object(backend._db, "relational_db")
    if hasattr(backend, "_qdrant"):
        backend._qdrant = instrument_object(backend._qdrant, "vector_db")
    if hasattr(backend, "_files"):
        backend._files = instrument_object(backend._files, "filesystem")

    # Chronos keeps the same logical clients behind its workspace facade.  The
    # SQL adapter is referenced by both the branch context and its internal
    # branching implementation, so replace both references.  ChronosFS calls
    # its native filesystem implementation directly; wrapping that object
    # captures block reads and writes without changing the POSIX-facing API.
    relational = getattr(backend, "relational", None)
    if relational is not None:
        adapter = getattr(relational, "_db", None)
        if adapter is not None and not isinstance(adapter, TimedStoreProxy):
            adapter = instrument_object(adapter, "relational_db")
            relational._db = adapter
        internal = getattr(relational, "_backend", None)
        if internal is not None and getattr(internal, "db", None) is not adapter:
            internal.db = adapter
        metadata = getattr(relational, "_metadata_db", None)
        if metadata is not None and not isinstance(metadata, TimedStoreProxy):
            relational._metadata_db = instrument_object(metadata, "relational_db")
        _instrument_chronos_context(relational, "relational_db")

    filesystem = getattr(backend, "filesystem", None)
    if filesystem is not None:
        # ChronosFS has a branch context of its own when its data database is
        # split from the shared workspace metadata database.  Its POSIX
        # operation is charged to the filesystem component below, while the
        # context's branch/interval SQL remains visible as relational time.
        filesystem_context = getattr(filesystem, "context", None)
        if filesystem_context is not None:
            filesystem_db = getattr(filesystem_context, "_db", None)
            if filesystem_db is not None and not isinstance(
                filesystem_db, TimedStoreProxy
            ):
                filesystem_db = instrument_object(
                    filesystem_db, "relational_db"
                )
                filesystem_context._db = filesystem_db
                internal = getattr(filesystem_context, "_backend", None)
                if internal is not None and getattr(internal, "db", None) is not filesystem_db:
                    internal.db = filesystem_db
            filesystem_metadata_db = getattr(
                filesystem_context, "_metadata_db", None
            )
            if filesystem_metadata_db is not None and not isinstance(
                filesystem_metadata_db, TimedStoreProxy
            ):
                filesystem_context._metadata_db = instrument_object(
                    filesystem_metadata_db, "relational_db"
                )
            _instrument_chronos_context(filesystem_context, "relational_db")
        native = getattr(filesystem, "_native", None)
        if native is not None and not isinstance(native, TimedStoreProxy):
            filesystem._native = instrument_object(native, "filesystem")
        # The public ChronosFS operation combines its filesystem engine with
        # relational metadata lookups.  Keep that outer operation in the
        # filesystem component while nested SQL calls retain their own
        # relational spans.  This makes the residual filesystem time visible
        # without double-counting the metadata calls.
        if not isinstance(filesystem, TimedStoreProxy):
            filesystem = instrument_object(filesystem, "filesystem")
            backend.filesystem = filesystem
            workspace = getattr(backend, "workspace", None)
            if workspace is not None:
                workspace.filesystem = filesystem

    qdrant = getattr(backend, "qdrant", None)
    if qdrant is not None:
        client = getattr(qdrant, "client", None)
        if client is not None and not isinstance(client, TimedStoreProxy):
            qdrant.client = instrument_object(client, "vector_db")
    return backend


_CONTEXT_METHODS = (
    "create_branch",
    "create_branch_from_checkpoint",
    "delete_branch",
    "list_branches",
    "get_branch",
    "checkout",
    "checkout_ref",
    "create_checkpoint",
    "diff",
    "merge_preview",
    "merge_apply",
)
_SESSION_METHODS = ("query", "execute", "explain", "rewrite_query")


def _instrument_chronos_session(session: Any, category: str) -> Any:
    for name in _SESSION_METHODS:
        original = getattr(session, name, None)
        if not callable(original) or getattr(original, "_chronos_timed", False):
            continue

        @functools.wraps(original)
        def invoke(*args: Any, __original: Any = original, **kwargs: Any) -> Any:
            with timed_store_call(category):
                return __original(*args, **kwargs)

        invoke._chronos_timed = True  # type: ignore[attr-defined]
        setattr(session, name, invoke)

    original_transaction = getattr(session, "transaction", None)
    if callable(original_transaction) and not getattr(
        original_transaction, "_chronos_timed", False
    ):
        @functools.wraps(original_transaction)
        def transaction(*args: Any, **kwargs: Any) -> Any:
            @contextlib.contextmanager
            def scope() -> Iterator[None]:
                with timed_store_call(category):
                    with original_transaction(*args, **kwargs):
                        yield

            return scope()

        transaction._chronos_timed = True  # type: ignore[attr-defined]
        setattr(session, "transaction", transaction)
    return session


def _instrument_chronos_context(context: Any, category: str) -> None:
    for name in _CONTEXT_METHODS:
        original = getattr(context, name, None)
        if not callable(original) or getattr(original, "_chronos_timed", False):
            continue

        @functools.wraps(original)
        def invoke(*args: Any, __original: Any = original, **kwargs: Any) -> Any:
            with timed_store_call(category):
                result = __original(*args, **kwargs)
            if hasattr(result, "branch_id") and hasattr(result, "query"):
                _instrument_chronos_session(result, category)
            return result

        invoke._chronos_timed = True  # type: ignore[attr-defined]
        setattr(context, name, invoke)


__all__ = [
    "STORE_CATEGORIES",
    "StoreTimingCollector",
    "TimedStoreProxy",
    "current_collector",
    "instrument_object",
    "instrument_backend",
    "timed_store_call",
]
