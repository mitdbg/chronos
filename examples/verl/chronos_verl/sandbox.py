"""Database state owned by one rollout, independent of individual tool calls."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from uuid import uuid4

from chronos_core.branching import ChronosBranchContext


class DatabaseEpisode:
    def __init__(self, url: str, checkpoint: str):
        self.url = url
        self.checkpoint = checkpoint
        self.branch = "verl_" + uuid4().hex
        # Keep the connection on one thread; do not block the Ray event loop.
        # Calls within an episode serialize, while episodes execute independently.
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="chronos-verl")
        self.context = None
        self.session = None
        self.created = False
        self.closed = False

    async def _submit(self, fn, *args):
        if self.closed:
            raise RuntimeError("episode is closed")
        future = asyncio.get_running_loop().run_in_executor(self.executor, partial(fn, *args))
        # A cancelled waiter cannot cancel a database transaction half way through.
        # close() is queued on this same executor after any in-flight operation.
        return await asyncio.shield(future)

    def _open(self):
        self.context = ChronosBranchContext.connect(self.url, backend="interval")
        self.context.create_branch_from_checkpoint(self.branch, checkpoint=self.checkpoint)
        self.created = True
        self.session = self.context.checkout(self.branch)

    async def __aenter__(self):
        try:
            await self._submit(self._open)
            return self
        except BaseException:
            await self.close()
            raise

    def _execute(self, operation, ticket_id, priority):
        if operation == "list":
            return self.session.query("SELECT id, subject, priority FROM tickets ORDER BY id")
        if operation != "set_priority":
            raise ValueError("operation must be list or set_priority")
        if type(ticket_id) is not int or priority not in ("low", "normal", "high"):
            raise ValueError("set_priority requires an integer ticket_id and low, normal, or high priority")
        with self.session.transaction():
            self.session.execute(
                "UPDATE tickets SET priority = :priority WHERE id = :id",
                {"id": ticket_id, "priority": priority},
            )
            return self.session.query(
                "SELECT id, subject, priority FROM tickets WHERE id = :id", {"id": ticket_id}
            )

    async def execute(self, operation, ticket_id=None, priority=None):
        return await self._submit(self._execute, operation, ticket_id, priority)

    async def score(self):
        rows = await self.execute("list")
        return float([(r["id"], r["priority"]) for r in rows] == [(101, "high"), (102, "normal")])

    def _close(self):
        try:
            if self.session is not None:
                self.session.close()
            if self.created:
                self.context.delete_branch(self.branch)
                self.created = False
        finally:
            if self.context is not None:
                self.context.close()

    async def close(self):
        if self.closed:
            return
        future = asyncio.get_running_loop().run_in_executor(self.executor, self._close)
        self.closed = True
        try:
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                await future
                raise
        finally:
            self.executor.shutdown(wait=False)

    async def __aexit__(self, *exc):
        await self.close()
