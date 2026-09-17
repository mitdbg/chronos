"""State owned by one verl rollout, independent of individual tool calls."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import shlex
import warnings
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any
from uuid import uuid4


def _validate(operation: str, ticket_id: int | None, priority: str | None) -> None:
    if operation == "list":
        return
    if operation != "set_priority":
        raise ValueError("operation must be list or set_priority")
    if type(ticket_id) is not int or priority not in ("low", "normal", "high"):
        raise ValueError(
            "set_priority requires an integer ticket_id and low, normal, or high priority"
        )


class TicketEpisode:
    """Serialize blocking state operations without blocking verl's event loop."""

    def __init__(self) -> None:
        self.branch = "verl_" + uuid4().hex
        self.executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="chronos-verl",
        )
        self.closed = False

    async def _submit(self, fn: Any, *args: Any) -> Any:
        if self.closed:
            raise RuntimeError("episode is closed")
        future = asyncio.get_running_loop().run_in_executor(
            self.executor,
            partial(fn, *args),
        )
        # A cancelled waiter cannot interrupt a database transaction or leave a
        # sandbox half-mounted. close() queues behind this operation.
        return await asyncio.shield(future)

    async def __aenter__(self) -> TicketEpisode:
        try:
            await self._submit(self._open)
            return self
        except BaseException:
            try:
                await self.close()
            except BaseException as cleanup_error:
                warnings.warn(
                    f"episode cleanup after setup failure also failed: {cleanup_error}",
                    RuntimeWarning,
                    stacklevel=2,
                )
            raise

    async def execute(
        self,
        operation: str,
        ticket_id: int | None = None,
        priority: str | None = None,
    ) -> list[dict[str, Any]]:
        return await self._submit(self._execute, operation, ticket_id, priority)

    async def score(self) -> float:
        rows = await self.execute("list")
        final_state = [(row["id"], row["priority"]) for row in rows]
        return float(final_state == [(101, "high"), (102, "normal")])

    def trajectory_metadata(self) -> dict[str, str]:
        return {"chronos_branch": self.branch, "chronos_backend": "local"}

    async def close(self) -> None:
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

    async def __aexit__(
        self,
        exception_type: Any,
        exception: Any,
        traceback: Any,
    ) -> None:
        if exception is None:
            await self.close()
            return
        try:
            await self.close()
        except BaseException as cleanup_error:
            warnings.warn(
                f"episode cleanup after rollout failure also failed: {cleanup_error}",
                RuntimeWarning,
                stacklevel=2,
            )

    def _open(self) -> None:
        raise NotImplementedError

    def _execute(
        self,
        operation: str,
        ticket_id: int | None,
        priority: str | None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _close(self) -> None:
        raise NotImplementedError


class DatabaseEpisode(TicketEpisode):
    """A lightweight rollout branch used directly by the verl worker."""

    def __init__(self, url: str, checkpoint: str):
        super().__init__()
        self.url = url
        self.checkpoint = checkpoint
        self.context: Any | None = None
        self.session: Any | None = None
        self.created = False

    def _open(self) -> None:
        from chronos_core.branching import ChronosBranchContext

        self.context = ChronosBranchContext.connect(self.url, backend="interval")
        self.context.create_branch_from_checkpoint(
            self.branch,
            checkpoint=self.checkpoint,
        )
        self.created = True
        self.session = self.context.checkout(self.branch)

    def _execute(
        self,
        operation: str,
        ticket_id: int | None,
        priority: str | None,
    ) -> list[dict[str, Any]]:
        _validate(operation, ticket_id, priority)
        assert self.session is not None
        if operation == "list":
            return self.session.query(
                "SELECT id, subject, priority FROM tickets ORDER BY id"
            )
        with self.session.transaction():
            self.session.execute(
                "UPDATE tickets SET priority = :priority WHERE id = :id",
                {"id": ticket_id, "priority": priority},
            )
            return self.session.query(
                "SELECT id, subject, priority FROM tickets WHERE id = :id",
                {"id": ticket_id},
            )

    def _close(self) -> None:
        try:
            if self.session is not None:
                self.session.close()
            if self.created:
                assert self.context is not None
                self.context.delete_branch(self.branch)
                self.created = False
        finally:
            if self.context is not None:
                self.context.close()


class E2BDatabaseEpisode(TicketEpisode):
    """A rollout running inside E2B on a branch supplied by chronos-gateway."""

    def __init__(
        self,
        *,
        template: str,
        workspace: str,
        database: str,
        from_branch: str = "main",
        filesystem: str = "read_write",
        ttl_seconds: int = 3600,
        timeout: int | None = None,
        mountpoint: str = "/mnt/chronos",
        command_timeout: int = 60,
        adapter: Any | None = None,
        sandbox_kwargs: dict[str, Any] | None = None,
        prepare: Any | None = None,
    ):
        super().__init__()
        self.template = template
        self.workspace = workspace
        self.database = database
        self.from_branch = from_branch
        self.filesystem = filesystem
        self.ttl_seconds = ttl_seconds
        self.timeout = timeout
        self.mountpoint = mountpoint.rstrip("/")
        self.command_timeout = command_timeout
        self.adapter = adapter
        self.sandbox_kwargs = dict(sandbox_kwargs or {})
        self.prepare = prepare
        self.rollout: Any | None = None

    def _open(self) -> None:
        if self.adapter is None:
            from chronos_e2b import ChronosE2B

            self.adapter = ChronosE2B.from_env()
        branch = self.adapter.branch(
            template=self.template,
            workspace=self.workspace,
            branch_id=self.branch,
            from_branch=self.from_branch,
            databases={self.database: "CHRONOS_DATABASE_URL"},
            filesystem=self.filesystem,
            ttl_seconds=self.ttl_seconds,
            timeout=self.timeout,
            mountpoint=self.mountpoint,
            metadata={"verl_episode": self.branch},
            sandbox_kwargs=self.sandbox_kwargs,
            prepare=self.prepare,
        )
        self.rollout = branch.start()

    def _sandbox_command(
        self,
        command: str,
        *,
        envs: dict[str, str] | None = None,
    ) -> str:
        if self.rollout is None:
            raise RuntimeError("E2B episode is not running")
        result = self.rollout.run(
            command,
            timeout=self.command_timeout,
            envs=envs,
        )
        if result.exit_code != 0:
            raise RuntimeError(
                f"E2B tool command failed ({result.exit_code})\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        return result.stdout

    def _query(self, sql: str) -> list[dict[str, Any]]:
        output = self._sandbox_command(
            'psql "$CHRONOS_DATABASE_URL" -X -v ON_ERROR_STOP=1 --csv '
            '-c "$CHRONOS_SQL"',
            envs={"CHRONOS_SQL": sql},
        )
        rows = list(csv.DictReader(io.StringIO(output)))
        try:
            return [
                {
                    "id": int(row["id"]),
                    "subject": row["subject"],
                    "priority": row["priority"],
                }
                for row in rows
            ]
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError(f"unexpected ticket query output: {output!r}") from error

    def _record(
        self,
        operation: str,
        ticket_id: int | None,
        priority: str | None,
    ) -> None:
        if self.filesystem == "none":
            return
        event = json.dumps(
            {
                "operation": operation,
                "ticket_id": ticket_id,
                "priority": priority,
            },
            separators=(",", ":"),
        )
        audit_path = shlex.quote(f"{self.mountpoint}/verl-tool-calls.jsonl")
        self._sandbox_command(
            f"printf '%s\\n' \"$CHRONOS_TOOL_EVENT\" >> {audit_path}",
            envs={"CHRONOS_TOOL_EVENT": event},
        )

    def _execute(
        self,
        operation: str,
        ticket_id: int | None,
        priority: str | None,
    ) -> list[dict[str, Any]]:
        _validate(operation, ticket_id, priority)
        if operation == "list":
            rows = self._query("SELECT id, subject, priority FROM tickets ORDER BY id")
        else:
            assert ticket_id is not None and priority is not None
            self._sandbox_command(
                'psql "$CHRONOS_DATABASE_URL" -X -v ON_ERROR_STOP=1 -q '
                '-c "$CHRONOS_SQL"',
                envs={
                    "CHRONOS_SQL": (
                        "UPDATE tickets SET priority = "
                        f"'{priority}' WHERE id = {ticket_id}"
                    )
                },
            )
            rows = self._query(
                f"SELECT id, subject, priority FROM tickets WHERE id = {ticket_id}"
            )
        self._record(operation, ticket_id, priority)
        return rows

    def _close(self) -> None:
        if self.rollout is not None:
            self.rollout.close()
            self.rollout = None

    def trajectory_metadata(self) -> dict[str, str]:
        metadata = {
            "chronos_branch": self.branch,
            "chronos_backend": "e2b",
            "chronos_workspace": self.workspace,
        }
        if self.rollout is not None and self.rollout.sandbox is not None:
            sandbox_id = getattr(self.rollout.sandbox, "sandbox_id", "")
            if sandbox_id:
                metadata["e2b_sandbox_id"] = sandbox_id
        return metadata
