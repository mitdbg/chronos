from __future__ import annotations

import os
import sys
import unittest
import warnings
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

if os.environ.get("CHRONOS_E2B_TEST_INSTALLED") != "1":
    sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from chronos_e2b import (  # noqa: E402
    ChronosCleanupError,
    ChronosControlError,
    ChronosE2B,
    ChronosE2BError,
)


@dataclass
class Result:
    exit_code: int = 0
    stdout: str = ""
    stderr: str = ""


class FakeCommands:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.fail_when: str | None = None

    def run(self, command: str, **kwargs: object) -> Result:
        self.calls.append((command, kwargs))
        if self.fail_when and self.fail_when in command:
            return Result(1, stderr="deliberate failure")
        if command == "hostname -I":
            return Result(stdout="10.42.0.18 192.168.1.9\n")
        return Result()


class FakeSandbox:
    def __init__(self, sandbox_id: str = "sandbox-1") -> None:
        self.sandbox_id = sandbox_id
        self.commands = FakeCommands()
        self.killed = False
        self.kill_error: BaseException | None = None

    def kill(self) -> None:
        self.killed = True
        if self.kill_error:
            raise self.kill_error


class FakeSandboxType:
    calls: list[tuple[str, dict[str, object]]] = []
    next_sandbox: FakeSandbox | None = None
    create_error: BaseException | None = None

    @classmethod
    def reset(cls) -> None:
        cls.calls = []
        cls.next_sandbox = None
        cls.create_error = None

    @classmethod
    def create(cls, template: str, **kwargs: object) -> FakeSandbox:
        cls.calls.append((template, kwargs))
        if cls.create_error:
            raise cls.create_error
        sandbox = cls.next_sandbox or FakeSandbox()
        cls.next_sandbox = sandbox
        return sandbox


class FakeControl:
    def __init__(self, workspace: str = "training", branch: str = "rollout-1"):
        self.workspace = workspace
        self.branch = branch
        self.calls: list[tuple[str, str, object]] = []
        self.fail_delete = False
        self.lose_create_response = False
        self.created = False

    def request(self, method: str, path: str, body: object = None) -> dict[str, object]:
        self.calls.append((method, path, body))
        if method == "POST" and path.endswith("/branches"):
            assert isinstance(body, dict)
            self.branch = str(body["branch_id"])
            self.created = True
            if self.lose_create_response:
                self.lose_create_response = False
                raise ChronosControlError(None, "connection reset after create")
            return {
                "workspace": self.workspace,
                "branch_id": self.branch,
                "database_urls": {
                    "orders": "postgresql://branch@host/orders",
                    "events": "postgresql://branch@host/events",
                },
            }
        if method == "GET" and self.created:
            return {
                "workspace": self.workspace,
                "branch_id": self.branch,
                "database_urls": {
                    "orders": "postgresql://branch@host/orders",
                    "events": "postgresql://branch@host/events",
                },
            }
        if method == "POST" and path.endswith("/attach"):
            return {
                "nfs_export": "10.0.0.1:/opaque",
                "nfs_options": "vers=4.2,port=2049,proto=tcp,hard,nosuid,nodev",
            }
        if method == "DELETE":
            if self.fail_delete:
                raise ChronosE2BError("delete failed")
            return {"workspace": self.workspace, "branch_id": self.branch}
        raise AssertionError((method, path, body))


class AdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        FakeSandboxType.reset()
        self.adapter = ChronosE2B(
            control_url="http://gateway.test:7400",
            controller_token="secret",
            sandbox_type=FakeSandboxType,
        )
        self.control = FakeControl()
        self.adapter._control = self.control

    def branch(self, **kwargs: object):
        arguments = {
            "template": "chronos-agent",
            "workspace": "training",
            "branch_id": "rollout-1",
            "databases": {"orders": "DATABASE_URL", "events": "EVENTS_URL"},
        }
        arguments.update(kwargs)
        return self.adapter.branch(**arguments)

    def test_complete_lifecycle(self) -> None:
        prepared: list[FakeSandbox] = []
        rollout = self.branch(
            envs={"PROMPT_ID": "p-1"},
            metadata={"episode": "7"},
            sandbox_kwargs={"api_url": "http://localhost:50001"},
            prepare=prepared.append,
        )
        with rollout as active:
            self.assertIs(active, rollout)
            self.assertEqual(
                active.database_urls["orders"], "postgresql://branch@host/orders"
            )
            result = active.run("python agent.py", timeout=0)
            self.assertEqual(result.exit_code, 0)

        sandbox = FakeSandboxType.next_sandbox
        assert sandbox is not None
        self.assertEqual(prepared, [sandbox])
        self.assertTrue(sandbox.killed)
        template, create = FakeSandboxType.calls[0]
        self.assertEqual(template, "chronos-agent")
        self.assertEqual(create["timeout"], 3600)
        self.assertEqual(create["api_url"], "http://localhost:50001")
        self.assertEqual(
            create["envs"],
            {
                "PROMPT_ID": "p-1",
                "DATABASE_URL": "postgresql://branch@host/orders",
                "EVENTS_URL": "postgresql://branch@host/events",
            },
        )
        self.assertEqual(
            create["metadata"],
            {
                "episode": "7",
                "chronos_workspace": "training",
                "chronos_branch": "rollout-1",
            },
        )
        commands = [command for command, _ in sandbox.commands.calls]
        self.assertIn("hostname -I", commands)
        self.assertTrue(
            any(command.startswith("mount -t nfs4") for command in commands)
        )
        self.assertTrue(any(command.startswith("sync ") for command in commands))
        self.assertTrue(any(command.startswith("umount ") for command in commands))
        self.assertEqual(
            [call[0] for call in self.control.calls], ["POST", "POST", "DELETE"]
        )
        attach = self.control.calls[1][2]
        self.assertEqual(
            attach,
            {"sandbox_id": "sandbox-1", "sandbox_ip": "10.42.0.18"},
        )

    def test_start_failure_kills_sandbox_and_closes_branch(self) -> None:
        sandbox = FakeSandbox()
        sandbox.commands.fail_when = "mount -t nfs4"
        FakeSandboxType.next_sandbox = sandbox
        with self.assertRaisesRegex(ChronosE2BError, "sandbox command failed"):
            self.branch().start()
        self.assertTrue(sandbox.killed)
        self.assertEqual(self.control.calls[-1][0], "DELETE")

    def test_sandbox_creation_failure_closes_branch(self) -> None:
        FakeSandboxType.create_error = RuntimeError("cannot create VM")
        with self.assertRaisesRegex(RuntimeError, "cannot create VM"):
            self.branch().start()
        self.assertEqual([call[0] for call in self.control.calls], ["POST", "DELETE"])

    def test_lost_create_response_is_reconciled_without_second_post(self) -> None:
        self.control.lose_create_response = True
        with self.branch(filesystem="none"):
            pass
        self.assertEqual(
            [call[0] for call in self.control.calls],
            ["POST", "GET", "DELETE"],
        )

    def test_no_filesystem_skips_attach_and_mount(self) -> None:
        with self.branch(filesystem="none") as rollout:
            sandbox = rollout.sandbox
            assert sandbox is not None
        self.assertEqual([call[0] for call in self.control.calls], ["POST", "DELETE"])
        self.assertFalse(
            any(command.startswith("mount ") for command, _ in sandbox.commands.calls)
        )

    def test_read_only_adds_client_side_mount_flag(self) -> None:
        with self.branch(filesystem="read_only") as rollout:
            sandbox = rollout.sandbox
            assert sandbox is not None
            mount = next(
                kwargs
                for command, kwargs in sandbox.commands.calls
                if command.startswith("mount -t nfs4")
            )
            self.assertTrue(str(mount["envs"]["CHRONOS_NFS_OPTIONS"]).endswith(",ro"))

    def test_revoke_uses_lazy_unmount_during_cleanup(self) -> None:
        with self.branch() as rollout:
            sandbox = rollout.sandbox
            assert sandbox is not None
            rollout.revoke()
        commands = [command for command, _ in sandbox.commands.calls]
        self.assertIn("umount -l /mnt/chronos", commands)
        self.assertFalse(any(command.startswith("sync ") for command in commands))
        self.assertEqual(
            [call[0] for call in self.control.calls], ["POST", "POST", "DELETE"]
        )

    def test_cleanup_failure_does_not_mask_body_exception(self) -> None:
        rollout = self.branch()
        with self.assertRaisesRegex(ValueError, "workload failed"):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                with rollout:
                    assert rollout.sandbox is not None
                    rollout.sandbox.kill_error = RuntimeError("kill failed")
                    self.control.fail_delete = True
                    raise ValueError("workload failed")
        self.assertTrue(any("cleanup failed" in str(item.message) for item in caught))

    def test_explicit_cleanup_reports_all_failures(self) -> None:
        rollout = self.branch().start()
        assert rollout.sandbox is not None
        rollout.sandbox.kill_error = RuntimeError("kill failed")
        self.control.fail_delete = True
        with self.assertRaises(ChronosCleanupError) as caught:
            rollout.close()
        self.assertEqual(len(caught.exception.errors), 2)

    def test_environment_collision_is_rejected_and_cleaned_up(self) -> None:
        with self.assertRaisesRegex(ChronosE2BError, "conflicts"):
            self.branch(envs={"DATABASE_URL": "unsafe"}).start()
        self.assertEqual([call[0] for call in self.control.calls], ["POST", "DELETE"])

    def test_from_env(self) -> None:
        variables = {
            "CHRONOS_CONTROL_URL": "http://gateway.internal:7400",
            "CHRONOS_CONTROLLER_TOKEN": "controller",
        }
        with patch.dict(os.environ, variables, clear=True):
            adapter = ChronosE2B.from_env(sandbox_type=FakeSandboxType)
        self.assertEqual(adapter._control.url, variables["CHRONOS_CONTROL_URL"])

    def test_argument_validation(self) -> None:
        with self.assertRaises(ValueError):
            self.branch(mountpoint="relative")
        with self.assertRaises(ValueError):
            self.branch(ttl_seconds=0)
        with self.assertRaises(ValueError):
            self.branch(databases={"one": "URL", "two": "URL"})
        with self.assertRaises(ValueError):
            self.branch(databases="orders")
        with self.assertRaises(ValueError):
            self.branch(sandbox_kwargs={"envs": {}})


if __name__ == "__main__":
    unittest.main()
