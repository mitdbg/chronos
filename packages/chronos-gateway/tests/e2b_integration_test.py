#!/usr/bin/env python3
"""End-to-end test against E2B Embed on the local machine.

The test starts a real Chronos gateway fixture, creates two real Firecracker
microVMs through the loopback E2B API, and verifies PostgreSQL and NFS branch
isolation from inside those microVMs. It never falls back to E2B Cloud.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch
from urllib.parse import urlparse

sys.path.insert(
    0,
    str(Path(__file__).parents[2] / "chronos-e2b" / "src"),
)
sys.path.insert(
    0,
    str(Path(__file__).parents[3] / "examples" / "verl"),
)

from chronos_e2b import BranchSandbox, ChronosE2B  # noqa: E402
from chronos_verl.sandbox import E2BDatabaseEpisode  # noqa: E402


SKIP = 77
WORKSPACE = "training"
DATABASE = "database"
SOURCE_BRANCH = "main"
TEMPLATE = "base"
TIMEOUT = 600


def unavailable(message: str) -> None:
    if os.environ.get("CHRONOS_E2B_REQUIRED") == "1":
        raise RuntimeError(message)
    print(f"SKIP: {message}", file=sys.stderr)
    raise SystemExit(SKIP)


def checked(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **kwargs,
    )


def read_local_e2b_environment() -> dict[str, str]:
    names = ("E2B_API_KEY", "E2B_API_URL", "E2B_SANDBOX_URL")
    supplied = {name: os.environ.get(name, "") for name in names}
    if not all(supplied.values()):
        embed = Path(os.environ.get("CHRONOS_E2B_EMBED_DIR", "/tmp/chronos-e2b-local"))
        compose = embed / "compose.yaml"
        if not compose.is_file():
            unavailable(
                f"local E2B Embed is not installed at {embed}; "
                "see docs/tutorials/e2b-sandbox.md"
            )
        try:
            result = checked(
                [
                    "docker",
                    "compose",
                    "--project-directory",
                    str(embed),
                    "exec",
                    "-T",
                    "ready",
                    "cat",
                    "/run/e2b/sdk.env",
                ]
            )
        except (OSError, subprocess.CalledProcessError) as error:
            unavailable(f"local E2B Embed is not ready: {error}")
        for line in result.stdout.splitlines():
            words = shlex.split(line)
            if len(words) == 2 and words[0] == "export" and "=" in words[1]:
                name, value = words[1].split("=", 1)
                if name in supplied:
                    supplied[name] = value

    missing = [name for name, value in supplied.items() if not value]
    if missing:
        unavailable("local E2B environment is missing " + ", ".join(missing))
    for name in ("E2B_API_URL", "E2B_SANDBOX_URL"):
        parsed = urlparse(supplied[name])
        if parsed.scheme not in {"http", "https"} or parsed.hostname not in {
            "127.0.0.1",
            "localhost",
            "::1",
        }:
            raise RuntimeError(
                f"{name} must point at the local E2B Embed stack, got {supplied[name]!r}"
            )
    return supplied


def host_address() -> str:
    configured = os.environ.get("CHRONOS_E2B_GATEWAY_HOST")
    if configured:
        return configured
    try:
        words = checked(["ip", "-4", "route", "get", "1.1.1.1"]).stdout.split()
        return words[words.index("src") + 1]
    except (OSError, ValueError, IndexError, subprocess.CalledProcessError) as error:
        raise RuntimeError(
            "cannot determine a host address reachable from the local E2B microVM; "
            "set CHRONOS_E2B_GATEWAY_HOST"
        ) from error


def unused_port(address: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind((address, 0))
        return listener.getsockname()[1]


@dataclass
class GatewayFixture:
    process: subprocess.Popen[str]
    process_group: int
    log: Any
    state: Path
    control_url: str
    controller_token: str

    def stop(self) -> None:
        def signal_group(signal: str) -> None:
            command = ["kill", signal, "--", f"-{self.process_group}"]
            if os.geteuid() != 0:
                command[:0] = ["sudo", "-n"]
            subprocess.run(
                command,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        signal_group("-TERM")
        if self.process.poll() is None:
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                signal_group("-KILL")
                self.process.wait(timeout=5)
        # sudo may exit before a child that it started. A final group signal
        # prevents that child from retaining the FUSE mount between test runs.
        signal_group("-KILL")
        self.log.close()
        mountpoint = self.state / "nfs" / "mount"
        subprocess.run(
            ["sudo", "-n", "umount", "-l", str(mountpoint)],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            shutil.rmtree(self.state)
        except PermissionError:
            subprocess.run(["sudo", "-n", "rm", "-rf", str(self.state)], check=False)


def start_gateway(gateway: Path, fixture: Path) -> GatewayFixture:
    if not gateway.is_file() or not fixture.is_file():
        raise RuntimeError("gateway test binaries were not built")
    address = host_address()
    control_port = unused_port(address)
    postgres_port = unused_port(address)
    nfs_port = unused_port(address)
    state = Path(tempfile.mkdtemp(prefix="chronos-e2b-", dir="/tmp"))
    checked([str(fixture), str(state)])

    ganesha_library = Path(
        os.environ.get(
            "CHRONOS_GATEWAY_NFS_LIBRARY", "/usr/lib/ganesha/libganesha_nfsd.so"
        )
    )
    ganesha_plugins = Path(
        os.environ.get(
            "CHRONOS_GATEWAY_NFS_PLUGINS", "/usr/lib/x86_64-linux-gnu/ganesha"
        )
    )
    if not ganesha_library.is_file() or not ganesha_plugins.is_dir():
        shutil.rmtree(state)
        unavailable("the local gateway test requires NFS-Ganesha and its VFS plugin")

    config = {
        "listeners": {
            "control": f"{address}:{control_port}",
            "postgres": f"{address}:{postgres_port}",
            "nfs": f"{address}:{nfs_port}",
        },
        "recovery_seconds": 0,
        "controller_token_env": "CHRONOS_CONTROLLER_TOKEN",
        "nfs": {
            "ganesha_library": str(ganesha_library),
            "plugins_directory": str(ganesha_plugins),
            "state_directory": str(state / "nfs"),
            "mountpoint": str(state / "nfs" / "mount"),
        },
        "workspaces": {
            WORKSPACE: {
                "metadata_url_env": "CHRONOS_METADATA_URL",
                "filesystem": {
                    "data_url_env": "CHRONOS_FILESYSTEM_DATA_URL",
                    "block_size": 4096,
                },
                "postgres": {DATABASE: {"data_url_env": "CHRONOS_DATABASE_URL"}},
            }
        },
    }
    config_path = state / "gateway.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    controller_token = uuid.uuid4().hex + uuid.uuid4().hex
    variables = {
        "CHRONOS_CONTROLLER_TOKEN": controller_token,
        "CHRONOS_METADATA_URL": f"sqlite://{state}/metadata.sqlite",
        "CHRONOS_DATABASE_URL": f"sqlite://{state}/database.sqlite",
        "CHRONOS_FILESYSTEM_DATA_URL": f"sqlite://{state}/filesystem.sqlite",
    }
    command = [str(gateway), "--config", str(config_path)]
    if os.geteuid() != 0:
        command = [
            "sudo",
            "-n",
            "env",
            *[f"{key}={value}" for key, value in variables.items()],
            *command,
        ]
        environment = None
    else:
        environment = os.environ.copy()
        environment.update(variables)
    log = (state / "gateway.log").open("w+", encoding="utf-8")
    process = subprocess.Popen(
        command,
        env=environment,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    running = GatewayFixture(
        process,
        process.pid,
        log,
        state,
        f"http://{address}:{control_port}",
        controller_token,
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            log.flush()
            log.seek(0)
            detail = log.read()
            running.stop()
            raise RuntimeError(f"Chronos gateway exited during startup:\n{detail}")
        try:
            with urllib.request.urlopen(running.control_url + "/healthz", timeout=1):
                return running
        except (OSError, urllib.error.URLError):
            time.sleep(0.1)
    running.stop()
    raise RuntimeError("Chronos gateway did not become healthy")


def run(sandbox: Any, command: str, *, root: bool = False, timeout: int = 60) -> str:
    result = sandbox.commands.run(
        command,
        user="root" if root else None,
        timeout=timeout,
    )
    if result.exit_code != 0:
        raise RuntimeError(
            f"sandbox command failed ({result.exit_code}): {command}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


def command_must_fail(sandbox: Any, command: str, *, timeout: int = 30) -> None:
    from e2b import CommandExitException

    try:
        result = sandbox.commands.run(command, timeout=timeout)
    except CommandExitException:
        return
    if result.exit_code == 0:
        raise RuntimeError(f"sandbox command unexpectedly succeeded: {command}")


def prepare_base_template(sandbox: Any) -> None:
    run(
        sandbox,
        "if ! command -v psql >/dev/null || ! command -v mount.nfs4 >/dev/null || "
        "! command -v rg >/dev/null; then "
        "apt-get update && DEBIAN_FRONTEND=noninteractive "
        "apt-get install -y --no-install-recommends "
        "postgresql-client nfs-common ripgrep; fi",
        root=True,
        timeout=300,
    )


async def exercise_verl_episode(
    adapter: ChronosE2B,
    sandbox_kwargs: dict[str, str],
) -> None:
    episode = E2BDatabaseEpisode(
        template=TEMPLATE,
        workspace=WORKSPACE,
        database=DATABASE,
        from_branch=SOURCE_BRANCH,
        filesystem="read_write",
        ttl_seconds=TIMEOUT + 300,
        timeout=TIMEOUT,
        adapter=adapter,
        sandbox_kwargs=sandbox_kwargs,
        prepare=prepare_base_template,
    )
    async with episode:
        initial = await episode.execute("list")
        if [(row["id"], row["priority"]) for row in initial] != [
            (101, "normal"),
            (102, "normal"),
        ]:
            raise RuntimeError(f"verl episode started from unexpected state: {initial}")
        await episode.execute("set_priority", 101, "high")
        if await episode.score() != 1.0:
            raise RuntimeError("verl episode did not score its branch-local update")
        assert episode.rollout is not None
        audit_lines = episode.rollout.run(
            "wc -l < /mnt/chronos/verl-tool-calls.jsonl",
        )
        if audit_lines.exit_code != 0 or audit_lines.stdout.strip() != "3":
            raise RuntimeError("verl episode did not retain its filesystem audit trail")


async def exercise_verl_agent_loop(
    adapter: ChronosE2B,
    sandbox_kwargs: dict[str, str],
) -> None:
    try:
        from chronos_verl.demo import run_demo
    except ImportError as error:
        unavailable(
            f"local E2B verl test requires the pinned verl environment: {error}"
        )
    variables = {
        "CHRONOS_VERL_BACKEND": "e2b",
        "CHRONOS_E2B_TEMPLATE": TEMPLATE,
        "CHRONOS_E2B_WORKSPACE": WORKSPACE,
        "CHRONOS_E2B_DATABASE": DATABASE,
        "CHRONOS_E2B_FROM_BRANCH": SOURCE_BRANCH,
    }
    with patch.dict(os.environ, variables):
        output, loop = await run_demo(
            e2b_adapter=adapter,
            e2b_prepare=prepare_base_template,
            e2b_sandbox_kwargs=sandbox_kwargs,
        )
    if output.reward_score != 1.0:
        raise RuntimeError("verl ToolAgentLoop did not return the expected reward")
    if output.extra_fields.get("chronos_backend") != "e2b":
        raise RuntimeError("verl trajectory did not use the E2B episode backend")
    if len(loop.observed) != 3:
        raise RuntimeError(
            "verl ToolAgentLoop did not dispatch all scripted tool calls"
        )


def sql_value(branch: BranchSandbox) -> str:
    assert branch.sandbox is not None
    return run(
        branch.sandbox,
        'psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -Atqc '
        '"SELECT value FROM chronos_gateway_e2e WHERE id = 1"',
    ).strip()


def exercise(gateway: GatewayFixture, sdk: dict[str, str], sandbox_type: Any) -> None:
    suffix = uuid.uuid4().hex[:12]
    adapter = ChronosE2B(
        control_url=gateway.control_url,
        controller_token=gateway.controller_token,
        sandbox_type=sandbox_type,
    )
    sandbox_kwargs = {
        "api_key": sdk["E2B_API_KEY"],
        "api_url": sdk["E2B_API_URL"],
        "sandbox_url": sdk["E2B_SANDBOX_URL"],
    }

    def rollout(branch_id: str) -> BranchSandbox:
        return adapter.branch(
            template=TEMPLATE,
            workspace=WORKSPACE,
            branch_id=branch_id,
            from_branch=SOURCE_BRANCH,
            databases={DATABASE: "DATABASE_URL"},
            filesystem="read_write",
            ttl_seconds=TIMEOUT + 300,
            timeout=TIMEOUT,
            sandbox_kwargs=sandbox_kwargs,
            prepare=prepare_base_template,
        )

    with contextlib.ExitStack() as stack:
        left = stack.enter_context(rollout(f"e2b-e2e/{suffix}/left"))
        right = stack.enter_context(rollout(f"e2b-e2e/{suffix}/right"))
        if sql_value(left) != "base" or sql_value(right) != "base":
            raise RuntimeError("fixture row must initially contain 'base'")

        assert left.sandbox is not None
        run(
            left.sandbox,
            'psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -qc '
            "\"UPDATE chronos_gateway_e2e SET value = 'left' WHERE id = 1\"",
        )
        run(
            left.sandbox,
            "mkdir /mnt/chronos/artifacts && "
            "printf '%s\\n' left > /mnt/chronos/artifacts/result.tmp",
        )
        run(
            left.sandbox,
            "cp /mnt/chronos/artifacts/result.tmp /mnt/chronos/result.copy",
        )
        run(
            left.sandbox,
            "mv /mnt/chronos/artifacts/result.tmp /mnt/chronos/result.txt && "
            "ln -s ../result.txt /mnt/chronos/artifacts/latest && "
            "chmod 640 /mnt/chronos/result.txt && "
            'test "$(cat /mnt/chronos/artifacts/latest)" = left && '
            "test \"$(sha256sum /mnt/chronos/result.copy | cut -d' ' -f1)\" = "
            "\"$(sha256sum /mnt/chronos/result.txt | cut -d' ' -f1)\"",
        )

        expected_search_hits = {
            "/mnt/chronos/result.copy",
            "/mnt/chronos/result.txt",
        }
        search_commands = {
            "grep": "grep -rlx left /mnt/chronos",
            "find": "find /mnt/chronos -type f -name 'result*'",
            "ripgrep": "rg -l '^left$' /mnt/chronos",
        }
        for tool, command in search_commands.items():
            hits = set(run(left.sandbox, command).splitlines())
            if hits != expected_search_hits:
                raise RuntimeError(
                    f"{tool} returned unexpected branch search results: {sorted(hits)}"
                )
        run(left.sandbox, "sync /mnt/chronos", root=True)
        ownership = run(
            left.sandbox,
            "printf '%s:%s/%s:%s' \"$(stat -c %u /mnt/chronos/result.txt)\" "
            '"$(stat -c %g /mnt/chronos/result.txt)" "$(id -u)" "$(id -g)"',
        ).strip()
        file_identity, process_identity = ownership.split("/", 1)
        if file_identity != process_identity:
            raise RuntimeError(
                "NFS file ownership does not match the creating process: " + ownership
            )
        run(left.sandbox, "chown 1234:2345 /mnt/chronos/result.txt", root=True)
        changed_owner = run(
            left.sandbox,
            "stat -c '%u:%g' /mnt/chronos/result.txt",
            root=True,
        ).strip()
        if changed_owner != "1234:2345":
            raise RuntimeError("NFS chown did not persist: " + changed_owner)
        run(left.sandbox, "chown 1000:1000 /mnt/chronos/result.txt", root=True)
        run(left.sandbox, "touch -d @123456789 /mnt/chronos/result.txt")
        visible_mtime = run(
            left.sandbox,
            "stat -c %Y /mnt/chronos/result.txt",
        ).strip()
        if visible_mtime != "123456789":
            raise RuntimeError("NFS timestamp update did not persist: " + visible_mtime)

        if sql_value(left) != "left":
            raise RuntimeError("left branch did not retain its database mutation")
        if sql_value(right) != "base":
            raise RuntimeError("database mutation leaked into the sibling branch")
        assert right.sandbox is not None
        run(
            right.sandbox,
            "test ! -e /mnt/chronos/result.txt && "
            "test ! -e /mnt/chronos/result.copy && "
            "test ! -e /mnt/chronos/artifacts",
        )
        file_value = run(left.sandbox, "cat /mnt/chronos/result.txt").strip()
        if file_value != "left":
            first_stat = run(
                left.sandbox,
                "stat -c 'size=%s blocks=%b' /mnt/chronos/result.txt",
            ).strip()
            time.sleep(2)
            second_value = run(left.sandbox, "cat /mnt/chronos/result.txt").strip()
            second_stat = run(
                left.sandbox,
                "stat -c 'size=%s blocks=%b' /mnt/chronos/result.txt",
            ).strip()
            raise RuntimeError(
                "left branch did not retain its filesystem mutation: "
                f"expected 'left', got {file_value!r}; "
                f"first {first_stat}; after 2s got {second_value!r}, {second_stat}"
            )

        asyncio.run(exercise_verl_episode(adapter, sandbox_kwargs))
        asyncio.run(exercise_verl_agent_loop(adapter, sandbox_kwargs))
        untouched_ticket = run(
            right.sandbox,
            'psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -Atqc '
            '"SELECT priority FROM tickets WHERE id = 101"',
        ).strip()
        if untouched_ticket != "normal":
            raise RuntimeError("verl database mutation leaked into a sibling branch")

        left.revoke()
        command_must_fail(
            left.sandbox,
            'psql "$DATABASE_URL" -X -Atqc "SELECT 1"',
        )
        command_must_fail(left.sandbox, "touch /mnt/chronos/after-close")

        print(
            "local E2B Firecracker, verl ToolAgentLoop, PostgreSQL, and NFS "
            "branch integration passed"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    arguments = parser.parse_args()
    sdk = read_local_e2b_environment()
    try:
        from e2b import Sandbox
    except ImportError:
        unavailable(
            "local E2B test requires e2b==2.46.0 in the selected Python environment"
        )

    gateway = start_gateway(arguments.gateway, arguments.fixture)
    try:
        exercise(gateway, sdk, Sandbox)
    except Exception:
        gateway.log.flush()
        gateway.log.seek(0)
        detail = gateway.log.read()
        if detail:
            print("\nChronos gateway log:\n" + detail, file=sys.stderr)
        ganesha_log = gateway.state / "nfs" / "ganesha.log"
        if ganesha_log.is_file():
            print(
                "\nNFS-Ganesha log:\n" + ganesha_log.read_text(encoding="utf-8"),
                file=sys.stderr,
            )
        raise
    finally:
        gateway.stop()


if __name__ == "__main__":
    main()
